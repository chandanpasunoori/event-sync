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
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

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

			// TODO store blobs to persistant storage (local disk file, sql db, mongo, redis)
			blobs = append(blobs, blobName)
			// TODO store to go gob
			// labels: kill-handle
			log.Info().Int("messages", items).Msg("messages written to blob")
			// TODO handle longer than 600s pubsub timeout, may be dynamic ack ttl from sub or config?
			for _, ms := range messagesForAck {
				ms.Ack()
			}
			metrics.SyncEvent.Add(float64(items))
			metrics.SyncEventWithLabel.With(prometheus.Labels{"type": filter.Name}).Add(float64(items))
		}

		timeKeyWritersMap.Delete(timeKey)
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

// TODO use as callback function from parent and make generic to multiple destinations
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
	timeKeyWritersMap sync.Map
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
				// TODO check gob and load to bq if present
				// labels: kill-handle

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

					wr, ok := timeKeyWritersMap.Load(timeKey)
					if !ok {
						wr = writeToBlob(ctx, et, timeKey, filter, job, storageClient, bulkSize, loadToBigQuery)
						timeKeyWritersMap.Store(timeKey, wr)
					}
					wr.(chan *pubsub.Message) <- mx
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
					// TODO create separate topic and push all unknown events to it, with attributes and payload
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
