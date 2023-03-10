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

func SubscribePubsubAndPull(wg *sync.WaitGroup, job Job) chan *pubsub.Message {
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

		ctx := context.Background()
		client, err := pubsub.NewClient(ctx, job.Source.PubsubConfig.ProjectId)
		if err != nil {
			log.Fatal().Err(err).Msg("pubsub client error")
		}

		defer client.Close()
		for {
			sub := client.Subscription(subID)
			if ok, err := sub.Exists(ctx); err != nil {
				log.Fatal().Err(err).Msg("subscription error")
			} else {
				if !ok {
					log.Fatal().Err(err).Msg("subscription not available")
				}
			}
			log.Info().Msg("subscription started")
			sub.ReceiveSettings.MaxOutstandingMessages = job.Source.PubsubConfig.MaxOutstandingMessages
			// receive messages until the passed in context is done.
			err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				eventChannel <- msg
			})

			if err != nil && status.Code(err) != codes.Canceled {
				log.Fatal().Err(err).Msg("subscription receive error")
			}
		}
	}(job.Source.PubsubConfig.SubscriptionId)
	return eventChannel
}

func WaitAndBQSync(wg *sync.WaitGroup, job Job, eventChannel chan *pubsub.Message) {
	wg.Add(1)
	bqctx := context.Background()

	log := log.With().
		Str("taskName", job.Name).
		Str("subscriptionId", job.Source.PubsubConfig.SubscriptionId).
		Logger()

	bqclient, err := bigquery.NewClient(bqctx, job.Destination.BigqueryConfig.ProjectId)
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
				ctx := context.Background()
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
					err := inserters[k].Put(ctx, items)
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
	}()
}

func writeToBlob(et time.Time, timeKey, k string, job Job, storageClient *storage.Client, bulkSize int) chan *pubsub.Message {
	ch := make(chan *pubsub.Message, bulkSize)
	uid := uuid.New().String()
	fileId := 0
	waiting := time.Now()
	log := log.With().Str("eventType", k).Str("taskName", job.Name).Logger()
	go func() {
		for repeatWriter := true; repeatWriter; {
			//@todo validate stop when its more than a certain amount of time without any items
			items := int(0)
			messagesForAck := []*pubsub.Message{}
			hrKey := et.Hour()

			blobName := fmt.Sprintf("%s/%s/dt=%s/hr=%d/uid=%s/%d.json",
				job.Destination.GoogleStorageConfig.BlobPrefix,
				k,
				et.Format("2006-01-02"),
				hrKey,
				uid,
				fileId,
			)
			fileId++

			bucketRef := storageClient.Bucket(job.Destination.GoogleStorageConfig.Bucket)

			blobRef := bucketRef.Object(blobName)
			blob := blobRef.NewWriter(context.Background())

			blob.ObjectAttrs.ContentType = "application/json"

			log := log.With().Str("blobName", blobName).Logger()

			log.Info().Msg("blob writer started")

			du := time.Second * 120
			timer := time.NewTimer(du)
			for repeat := true; repeat; {
				select {
				case <-timer.C:
					repeat = false
					log.Info().Dur("duration", du).Msg(fmt.Sprintf("blob writer timed out, no events in last %s, breaking loop", du))
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
					log.Info().Dur("duration", du).Str("timeKey", timeKey).Str("uid", uid).Msg("writer stopping due to no new messages")
				}
				continue
			}

			waiting = time.Now()

			err := blob.Close()
			if err != nil {
				log.Fatal().Err(err).Str("blobName", blobName).Msg("unable to close blob writer")
			}

			for _, ms := range messagesForAck {
				ms.Ack()
			}
			metrics.SyncEvent.Add(float64(items))
			metrics.SyncEventWithLabel.With(prometheus.Labels{"type": k}).Add(float64(items))
		}
		writerMutex.Lock()
		delete(timeKeyWriters, timeKey)
		writerMutex.Unlock()
		close(ch)
		log.Info().Str("timeKey", timeKey).Str("uid", uid).Msg("writer stopped")
	}()
	return ch
}

var (
	timeKeyWriters = make(map[string]chan *pubsub.Message)
	writerMutex    sync.Mutex
)

func WaitAndGoogleStorageSync(wg *sync.WaitGroup, job Job, eventChannel chan *pubsub.Message) {
	wg.Add(1)
	storageCtx := context.Background()
	storageClient, err := storage.NewClient(storageCtx)
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
			messagesToIngest[filter.Name] = make(chan *pubsub.Message, buffer)
			it := 0
			go func(k string) {
				for mx := range messagesToIngest[k] {
					var event map[string]interface{}
					err := json.Unmarshal(mx.Data, &event)
					if err != nil {
						mx.Ack()
						log.Error().Err(err).Msg("unable to unmarshal event")
						continue
					}
					etNumber := event[job.Destination.TimestampColumnName].(float64)
					et := time.Unix(int64(etNumber), 0)

					timeKey := fmt.Sprintf("%s-%s", k, et.Format("2006-01-02-15"))

					wr, ok := timeKeyWriters[timeKey]
					if !ok {
						writerMutex.Lock()
						wr = writeToBlob(et, timeKey, k, job, storageClient, bulkSize)
						timeKeyWriters[timeKey] = wr
						writerMutex.Unlock()
					}
					wr <- mx
					it++
				}
			}(filter.Name)
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
	}()
}
