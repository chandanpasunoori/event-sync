package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/chandanpasunoori/event-sync/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type Event map[string]interface{}

func (data Event) Save() (map[string]bigquery.Value, string, error) {
	r := make(map[string]bigquery.Value)
	for k, v := range data {
		r[k] = v
	}
	return r, bigquery.NoDedupeID, nil
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
				defer func() {
					if r := recover(); r != nil {
						log.Error().Interface("panicValue", r).Msg("panicked")
					}
				}()
				for repeatWriter := true; repeatWriter; {
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
							repeatWriter = false
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
						if !repeatWriter {
							break
						}
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
