package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/chandanpasunoori/event-sync/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

type Event map[string]interface{}

func (data Event) Save() (map[string]bigquery.Value, string, error) {
	r := make(map[string]bigquery.Value)
	for k, v := range data {
		r[k] = v
	}
	return r, bigquery.NoDedupeID, nil
}

func SubscribePubsubAndPull(ctx context.Context, wg *sync.WaitGroup, job Job) chan *pubsub.Message {
	buffer := int(float64(job.Destination.BatchSize) * float64(10))
	eventChannel := make(chan *pubsub.Message, buffer)

	wg.Add(1)
	go func(subID string) {
		defer wg.Done()

		log := log.With().
			Str("taskName", job.Name).
			Int("bufferSize", buffer).
			Str("subscriptionId", subID).
			Logger()

		log.Info().Msg("pubsub subscription starting")
		client, err := pubsub.NewClient(ctx, job.Source.PubsubConfig.ProjectId)
		if err != nil {
			log.Fatal().Err(err).Msg("pubsub client error")
		}

		defer client.Close()
		for {
			sub := client.Subscription(subID)
			ok, err := sub.Exists(ctx)
			if err != nil {
				if status.Code(err) == codes.Canceled {
					log.Warn().Str("reason", "application shutting down").Msg("subscription cancelled")
					break
				}
				log.Error().Err(err).Msg("subscription error")
				break
			}
			if !ok {
				log.Fatal().Err(err).Msg("subscription not available")
			}
			log.Info().Msg("subscription started")
			sub.ReceiveSettings.MaxOutstandingMessages = job.Source.PubsubConfig.MaxOutstandingMessages
			// receive messages until the passed in context is done.
			err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				eventChannel <- msg
			})
			if err != nil && status.Code(err) != codes.Canceled {
				log.Error().Err(err).Msg("subscription receive error")
			}
		}
		log.Error().Msg("closing main event channel")
		close(eventChannel)
		log.Error().Msg("closed main event channel")
	}(job.Source.PubsubConfig.SubscriptionId)

	return eventChannel
}

func WaitAndBQSync(ctx context.Context, wg *sync.WaitGroup, job Job, eventChannel chan *pubsub.Message) {
	wg.Add(1)

	log := log.With().
		Str("taskName", job.Name).
		Str("subscriptionId", job.Source.PubsubConfig.SubscriptionId).
		Logger()

	bqclient, err := bigquery.NewClient(context.TODO(), job.Destination.BigqueryConfig.ProjectId)
	if err != nil {
		log.Fatal().Err(err).Msg("bigquery client error")
	}
	defer bqclient.Close()

	ignored := uint64(0)

	inserters := make(map[string]*bigquery.Inserter)
	// Create a map of event tables to insert into
	eventsToIngest := make(map[string]bool)
	messagesToIngest := make(map[string]chan *pubsub.Message)
	buffer := int(float64(job.Destination.BatchSize) * float64(10))
	bulkSize := job.Destination.BatchSize

	log = log.With().
		Int("bufferSize", buffer).
		Int("bulkSize", bulkSize).
		Logger()

	log.Info().Msg("bq sync started")

	for _, filter := range job.Filters {
		log := log.With().Str("eventType", filter.Name).Logger()

		if filter.Action == "ingest" {
			inserters[filter.Name] = bqclient.Dataset(job.Destination.BigqueryConfig.Dataset).Table(filter.Target.Table).Inserter()
			messagesToIngest[filter.Name] = make(chan *pubsub.Message, buffer)
			go func(k string) {
				for {
					items := []Event{}
					messagesForAck := []*pubsub.Message{}
					du := time.Second * 10
					timer := time.NewTimer(du)
					for repeat := true; repeat; {
						select {
						case <-timer.C:
							repeat = false
							log.Info().Dur("duration", du).Msg(fmt.Sprintf("bq writer timed out, no events in last %s, breaking loop", du))
						case <-ctx.Done():
							repeat = false
							log.Info().Dur("duration", du).Msg("context is cancelled, breaking loop")
						case mx := <-messagesToIngest[k]:
							var data map[string]interface{}
							err := json.Unmarshal(mx.Data, &data)
							if err != nil {
								mx.Ack()
								log.Error().Err(err).Msg("unmarshal error")
								continue
							}
							items = append(items, data)
							messagesForAck = append(messagesForAck, mx)
							if len(items) >= bulkSize {
								repeat = false
							}
						}
					}
					timer.Stop()
					if len(items) <= 0 {
						log.Info().Dur("duration", du).Msg("items is empty, skipping bq submit")
						continue
					}
					err := inserters[k].Put(context.TODO(), items)
					if err != nil {
						for _, ms := range messagesForAck {
							ms.Ack()
						}
						log.Error().Err(err).Str("eventType", k).Msg("bigquery streaming put error")
					}
					for _, ms := range messagesForAck {
						ms.Ack()
					}

					log.Info().Int("count", len(items)).Msg("messaged pushed to bigquery")
					metrics.SyncEvent.Add(float64(len(items)))
					metrics.SyncEventWithLabel.With(prometheus.Labels{"type": k}).Add(float64(len(items)))
				}
			}(filter.Name)
			eventsToIngest[filter.Name] = true
			log.Info().Err(err).Str("filterName", filter.Name).Msg("filter registered")
		} else {
			eventsToIngest[filter.Name] = false
			log.Error().Err(err).Str("filterName", filter.Name).Msg("filter ignored")
		}
	}

	go func() {
		defer wg.Done()
		for msg := range eventChannel {
			if xtype, ok := msg.Attributes[job.Source.PubsubConfig.AttributeKeyName]; ok {
				if ingest, ok := eventsToIngest[xtype]; ok && !ingest {
					msg.Ack()
					ignored++
					log.Info().Uint64("ignored", ignored).Msg("ignored")
					continue
				}

				if _, ok := eventsToIngest[xtype]; ok {
					if _, ok := inserters[xtype]; ok {
						messagesToIngest[xtype] <- msg
					} else {
						msg.Ack()
						log.Error().Interface("attributes", msg.Attributes).Str("eventType", xtype).Msg("event type not found")
					}
				} else {
					msg.Ack()
					log.Error().Interface("attributes", msg.Attributes).Str("eventType", xtype).Msg("event type not found")
				}
			} else {
				msg.Ack()
				log.Error().Interface("attributes", msg.Attributes).Msg("event type not found")
			}
		}
		log.Error().Msg("closing all job/filter channels")
		for _, c := range messagesToIngest {
			close(c)
		}
	}()
}

func writeToBlob(ctx context.Context, et time.Time, timeKey string, filter Filter, job Job, storageClient *storage.Client, bulkSize int, loadToBigQuery bool) chan *pubsub.Message {
	ch := make(chan *pubsub.Message, bulkSize)
	uid := uuid.New().String()
	fileId := 0
	waiting := time.Now()
	log := log.With().Str("eventType", filter.Name).Str("taskName", job.Name).Logger()
	blobs := make([]string, 0)
	go func() {
		for repeatWriter := true; repeatWriter; {
			items := int(0)
			messagesForAck := []*pubsub.Message{}
			hrKey := et.Hour()

			blobName := fmt.Sprintf("%s/%s/dt=%s/hr=%d/uid=%s/%d.json",
				job.Destination.GoogleStorageConfig.BlobPrefix,
				filter.Name,
				et.Format("2006-01-02"),
				hrKey,
				uid,
				fileId,
			)
			fileId++

			bucketRef := storageClient.Bucket(job.Destination.GoogleStorageConfig.Bucket)

			blobRef := bucketRef.Object(blobName)
			blob := blobRef.NewWriter(context.TODO())

			blob.ObjectAttrs.ContentType = "application/json"

			log := log.With().Str("blobName", blobName).Str("timeKey", timeKey).Str("uid", uid).Logger()

			log.Info().Msg("blob writer started")

			du := time.Second * 120
			timer := time.NewTimer(du)
			for repeat := true; repeat; {
				select {
				case <-timer.C:
					repeat = false
					log.Info().Dur("duration", du).Msgf("blob writer timed out, no events in last %s, breaking loop", du)
				case <-ctx.Done():
					repeat = false
					repeatWriter = false
					log.Info().Dur("duration", du).Msg("context is cancelled, breaking loop")
				case mx := <-ch:
					if _, err := blob.Write(mx.Data); err != nil {
						mx.Nack()
						log.Fatal().Err(err).Msg("unable to write to blob writer")
					}
					if _, err := blob.Write([]byte("\n")); err != nil {
						mx.Nack()
						log.Fatal().Err(err).Msg("unable to write to blob writer")
					}
					items++
					messagesForAck = append(messagesForAck, mx)
					if items >= bulkSize {
						repeat = false
					}
				}
			}
			timer.Stop()
			if items == 0 {
				log.Info().Dur("duration", du).Msg("items is empty, skipping blob save")
				if time.Since(waiting).Minutes() >= 5 {
					repeatWriter = false
					log.Info().Dur("duration", du).Dur("waited", time.Since(waiting)).Msg("writer stopping due to no new messages")
				}
				continue
			}

			waiting = time.Now()

			err := blob.Close()
			if err != nil {
				log.Fatal().Err(err).Msg("unable to close blob writer")
			}

			// @todo: store blobs to persistant storage (local disk file, sql db, mongo, redis)
			blobs = append(blobs, blobName)
			// @todo: handle longer than 600s pubsub timeout, may be dynamic ack ttl from sub or config?
			for _, ms := range messagesForAck {
				ms.Ack()
			}
			metrics.SyncEvent.Add(float64(items))
			metrics.SyncEventWithLabel.With(prometheus.Labels{"type": filter.Name}).Add(float64(items))
		}
		writerMutex.Lock()
		delete(timeKeyWriters, timeKey)
		writerMutex.Unlock()
		close(ch)
		log.Info().Msg("writer stopped")

		if loadToBigQuery {
			if len(blobs) > 0 {
				loadToBigQueryJob(job, log, blobs, filter)
			}
		}
	}()
	return ch
}

// @todo: use as callback function from parent and make generic to multiple destinations
func loadToBigQueryJob(job Job, log zerolog.Logger, blobs []string, filter Filter) {
	bqclient, err := bigquery.NewClient(context.TODO(), job.Destination.BigqueryConfig.ProjectId)
	if err != nil {
		log.Fatal().Err(err).Msg("bigquery client error")
	}
	defer bqclient.Close()

	blobRef := make([]string, 0)
	for _, b := range blobs {
		blobRef = append(blobRef, fmt.Sprintf("gs://%s/%s", job.Destination.GoogleStorageConfig.Bucket, b))
	}

	log.Info().Strs("blobs", blobs).Strs("blobRef", blobRef).Str("table", filter.Target.Table).Str("dataset", job.Destination.BigqueryConfig.Dataset).Msg("blobs loading to bigquery")

	schemaTpl, err := json.Marshal(filter.Schema)
	if err != nil {
		log.Fatal().Err(err).Msg("bigquery schema parsing error")
	}
	schema, err := bigquery.SchemaFromJSON(schemaTpl)
	if err != nil {
		log.Fatal().Err(err).Msg("bigquery schema parsing error")
	}

	gcsRef := bigquery.NewGCSReference(blobRef...)
	gcsRef.IgnoreUnknownValues = true
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = schema
	loader := bqclient.Dataset(job.Destination.BigqueryConfig.Dataset).Table(filter.Target.Table).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.CreateDisposition = bigquery.CreateIfNeeded

	if len(job.Destination.ClusterBy) > 0 {
		loader.Clustering = &bigquery.Clustering{
			Fields: job.Destination.ClusterBy,
		}
	}
	if len(job.Destination.TimePartitioningType) > 0 {
		expire := time.Second * 0
		if len(job.Destination.Expiration) > 0 {
			expire, err = time.ParseDuration(job.Destination.Expiration)
			if err != nil {
				log.Fatal().Err(err).Str("partitionType", job.Destination.TimePartitioningType).Msg("bigquery invalid partitioning error")
			}
		}

		pType := bigquery.DayPartitioningType
		switch job.Destination.TimePartitioningType {
		case "HOUR":
			pType = bigquery.HourPartitioningType
		case "DAY":
			pType = bigquery.DayPartitioningType
		case "MONTH":
			pType = bigquery.MonthPartitioningType
		case "YEAR":
			pType = bigquery.YearPartitioningType
		default:
			log.Fatal().Str("partitionType", job.Destination.TimePartitioningType).Msg("bigquery invalid partitioning error")
		}
		loader.TimePartitioning = &bigquery.TimePartitioning{
			Field:                  job.Destination.TimestampColumnName,
			RequirePartitionFilter: true,
			Type:                   pType,
			Expiration:             expire,
		}
	}

	bqJob, err := loader.Run(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msg("bigquery load error")
	}
	status, err := bqJob.Wait(context.TODO())
	if err != nil {
		log.Fatal().Err(err).Msg("bigquery load error")
	}
	if status.Err() != nil {
		log.Fatal().Interface("errors", status.Errors).Err(status.Err()).Msg("job completed with errors")
	}
	log.Info().Msg("loading to bigquery completed")
}

var (
	timeKeyWriters = make(map[string]chan *pubsub.Message)
	writerMutex    sync.Mutex
)

func WaitAndGoogleStorageSync(ctx context.Context, wg *sync.WaitGroup, job Job, eventChannel chan *pubsub.Message, loadToBigQuery bool) {
	wg.Add(1)
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("google storage client client error")
	}

	ignored := uint64(0)

	// Create a map of event tables to insert into
	eventsToIngest := make(map[string]bool)
	messagesToIngest := make(map[string]chan *pubsub.Message)
	buffer := int(float64(job.Destination.BatchSize) * float64(10))
	bulkSize := job.Destination.BatchSize

	log.Info().
		Str("taskName", job.Name).
		Str("subscriptionId", job.Source.PubsubConfig.SubscriptionId).
		Int("bufferSize", buffer).
		Int("bulkSize", bulkSize).
		Msg("google storage sync started")

	for _, filter := range job.Filters {
		log := log.With().Str("eventType", filter.Name).Logger()
		if filter.Action == "ingest" {

			if loadToBigQuery {
				//to fail early if schema/config is invalid
				schemaTpl, err := json.Marshal(filter.Schema)
				if err != nil {
					log.Fatal().Err(err).Msg("bigquery schema parsing error")
				}
				_, err = bigquery.SchemaFromJSON(schemaTpl)
				if err != nil {
					log.Fatal().Err(err).Msg("bigquery schema parsing error")
				}
				if len(job.Destination.Expiration) > 0 {
					_, err := time.ParseDuration(job.Destination.Expiration)
					if err != nil {
						log.Fatal().Err(err).Str("expiration", job.Destination.Expiration).Msg("bigquery invalid expiration error")
					}
				}
				if len(job.Destination.TimePartitioningType) > 0 {
					switch job.Destination.TimePartitioningType {
					case "HOUR":
					case "DAY":
					case "MONTH":
					case "YEAR":
					default:
						log.Fatal().Str("partitionType", job.Destination.TimePartitioningType).Msg("bigquery invalid partitioning error")
					}
				}
			}

			messagesToIngest[filter.Name] = make(chan *pubsub.Message, buffer)
			it := 0
			go func(filter Filter) {
				for mx := range messagesToIngest[filter.Name] {
					var event map[string]interface{}
					err := json.Unmarshal(mx.Data, &event)
					if err != nil {
						mx.Ack()
						log.Error().Err(err).Msg("unable to unmarshal event")
						continue
					}

					var et time.Time
					if job.Destination.TimestampFormat == "epoch" {
						etNumber := event[job.Destination.TimestampColumnName].(float64)
						et = time.Unix(int64(etNumber), 0)
					} else {
						if etNumber, ok := event[job.Destination.TimestampColumnName].(string); ok {
							et, err = time.Parse(job.Destination.TimestampFormat, etNumber)
							if err != nil {
								log.Error().Err(err).Msg("unable to unmarshal event timestamp")
								mx.Nack()
								continue
							}
						} else {
							mx.Nack()
							et = time.Now()
						}
					}

					timeKey := fmt.Sprintf("%s-%s", filter.Name, et.Format("2006-01-02-15-04"))
					timeKey = timeKey[:len(timeKey)-1] //10 minute buckets

					wr, ok := timeKeyWriters[timeKey]
					if !ok {
						writerMutex.Lock()
						wr = writeToBlob(ctx, et, timeKey, filter, job, storageClient, bulkSize, loadToBigQuery)
						timeKeyWriters[timeKey] = wr
						writerMutex.Unlock()
					}
					wr <- mx
					it++
				}
			}(filter)
			eventsToIngest[filter.Name] = true
		} else {
			eventsToIngest[filter.Name] = false
		}
	}

	go func() {
		defer wg.Done()
		for msg := range eventChannel {
			if xtype, ok := msg.Attributes[job.Source.PubsubConfig.AttributeKeyName]; ok {
				if ingest, ok := eventsToIngest[xtype]; ok && !ingest {
					msg.Ack()
					ignored++
					continue
				}

				if _, ok := eventsToIngest[xtype]; ok {
					messagesToIngest[xtype] <- msg
				} else {
					// @todo: create separate topic and push all unknown events to it, with attributes and payload
					msg.Ack()
					log.Error().Interface("attributes", msg.Attributes).Str("eventType", xtype).Msg("event type not found")
				}
			} else {
				msg.Ack()
				log.Error().Interface("attributes", msg.Attributes).Msg("event type not found")
			}
		}
		log.Error().Msg("closing all job/filter channels")
		for _, c := range messagesToIngest {
			close(c)
		}
		log.Error().Msg("closed all job/filter channels")
	}()
}
