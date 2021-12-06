package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

var logger = log.Logger{
	Out: os.Stdout,
	Formatter: &log.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
	},
	Level: log.InfoLevel,
}

type Event map[string]interface{}

func (data Event) Save() (map[string]bigquery.Value, string, error) {
	r := make(map[string]bigquery.Value)
	for k, v := range data {
		r[k] = v
	}
	return r, bigquery.NoDedupeID, nil
}

func SubscribePubsubAndPull(wg *sync.WaitGroup, job Job) chan *pubsub.Message {
	buffer := int(float64(job.Destination.BatchSize) * float64(2))
	eventChannel := make(chan *pubsub.Message, buffer)

	wg.Add(1)
	go func(subID string) {
		defer wg.Done()

		logger.Info("pubsub subscription starting for task: ", job.Name, " subscriptionId: ", job.Source.PubsubConfig.SubscriptionId, " with channel buffer: ", buffer)
		ctx := context.Background()
		client, err := pubsub.NewClient(ctx, job.Source.PubsubConfig.ProjectId)
		if err != nil {
			logger.Fatalln(fmt.Errorf("pubsub.NewClient: %v", err))
		}

		defer client.Close()
		for {
			sub := client.Subscription(subID)
			if ok, err := sub.Exists(ctx); err != nil {
				logger.Fatalln(fmt.Errorf("subscription error: %v", err))
			} else {
				if !ok {
					logger.Fatalln(fmt.Errorf("subscription %s not available", subID))
				}
			}
			logger.Info("subscription started for task: ", job.Name)
			// sub.ReceiveSettings.Synchronous = true
			sub.ReceiveSettings.MaxOutstandingMessages = job.Source.PubsubConfig.MaxOutstandingMessages
			// receive messages until the passed in context is done.
			err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
				eventChannel <- msg
			})
			if err != nil && status.Code(err) != codes.Canceled {
				logger.Fatalln(fmt.Errorf("receive: %v", err))
			}
		}
	}(job.Source.PubsubConfig.SubscriptionId)
	return eventChannel
}

func WaitAndBQSync(wg *sync.WaitGroup, job Job, eventChannel chan *pubsub.Message) {
	wg.Add(1)
	bqctx := context.Background()
	bqclient, err := bigquery.NewClient(bqctx, job.Destination.BigqueryConfig.ProjectId)
	if err != nil {
		logger.Fatalln(fmt.Errorf("bigquery client error: %v", err))
	}
	defer bqclient.Close()

	ignored := uint64(0)

	inserters := make(map[string]*bigquery.Inserter)
	// Create a map of event tables to insert into
	eventsToIngest := make(map[string]bool)
	messagesToIngest := make(map[string]chan *pubsub.Message)
	buffer := int(float64(job.Destination.BatchSize) * float64(1.25))
	bulkSize := job.Destination.BatchSize
	logger.Info("bq sync started for task: ", job.Name, " with bulkSize: ", bulkSize, " with buffer: ", buffer)

	for _, filter := range job.Filters {
		if filter.Action == "ingest" {
			inserters[filter.Name] = bqclient.Dataset(job.Destination.BigqueryConfig.Dataset).Table(filter.Target.Table).Inserter()
			messagesToIngest[filter.Name] = make(chan *pubsub.Message, buffer)
			go func(k string) {
				ctx := context.Background()
				synced := 0
				started := time.Now()
				for {
					items := []Event{}
					messagesForAck := []*pubsub.Message{}
					du := time.Second * 10
					timer := time.NewTimer(du)
					for repeat := true; repeat; {
						select {
						case <-timer.C:
							repeat = false
							logger.Info("job: ", job.Name, " event_type: ", k, " ", du.Seconds(), " seconds elapsed, breaking loop")
						case mx := <-messagesToIngest[k]:
							var data map[string]interface{}
							err := json.Unmarshal(mx.Data, &data)
							if err != nil {
								logger.Info("payload: ", string(mx.Data))
								logger.Fatalln(fmt.Errorf("json.Unmarshal: %v", err))
							}
							items = append(items, data)
							messagesForAck = append(messagesForAck, mx)
							if len(items) >= bulkSize {
								repeat = false
							}
						}
					}
					timer.Stop()
					if len(items) == 0 {
						logger.Info("job: ", job.Name, " event_type: ", k, " items is empty, skipping insert")
						continue
					}
					err := inserters[k].Put(ctx, items)
					if err != nil {
						for _, ms := range messagesForAck {
							ms.Nack()
						}
						logger.Fatalln(fmt.Errorf("%s.Put: %v", k, err))
					}
					for _, ms := range messagesForAck {
						ms.Ack()
					}
					synced += len(messagesForAck)
					mps := float64(synced) / time.Since(started).Seconds()
					logger.Info("event_type: ", k, " synced: ", synced, " mps: ", mps)
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
					if _, ok := inserters[xtype]; ok {
						messagesToIngest[xtype] <- msg
					} else {
						msg.Nack()
						logger.Errorln("attributes: ", msg.Attributes, ", event: ", xtype, ", payload: ", string(msg.Data))
						logger.Fatalln(fmt.Sprintf("event type %s not found", xtype))
					}
				} else {
					msg.Nack()
					logger.Errorln("attributes: ", msg.Attributes, ", event: ", xtype, ", payload: ", string(msg.Data))
					logger.Fatalln(fmt.Sprintf("event type %s not found", xtype))
				}
			}
		}
	}()
}

func writeToBlob(et time.Time, timeKey, k string, job Job, storageClient *storage.Client, bulkSize int) chan *pubsub.Message {
	ch := make(chan *pubsub.Message, bulkSize)
	uid := uuid.New().String()
	fileId := 0
	waiting := time.Now()
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

			logger.Info("blobName started: ", blobName)

			du := time.Second * 120
			timer := time.NewTimer(du)
			for repeat := true; repeat; {
				select {
				case <-timer.C:
					repeat = false
					logger.Info("job: ", job.Name, " event_type: ", k, " ", du.Seconds(), " seconds elapsed, breaking loop")
				case mx := <-ch:
					if _, err := blob.Write(mx.Data); err != nil {
						mx.Nack()
						logger.Info("error: ", err)
						logger.Fatalln(fmt.Errorf("blob.Write: %v", err))
					}
					if _, err := blob.Write([]byte("\n")); err != nil {
						mx.Nack()
						logger.Info("error: ", err)
						logger.Fatalln(fmt.Errorf("blob.Write: %v", err))
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
				logger.Info("job: ", job.Name, " event_type: ", k, " items is empty, skipping blob save")
				if time.Since(waiting).Minutes() >= 5 {
					repeatWriter = false
					logger.Info("job: ", job.Name, " event_type: ", k, " time key: ", timeKey, " uid: ", uid, " writer stopping due to no new messages")
				}
				continue
			}

			waiting = time.Now()

			err := blob.Close()
			if err != nil {
				logger.Info("error: ", err)
				logger.Fatalln(fmt.Errorf("blob.Close: %v", err))
			}

			for _, ms := range messagesForAck {
				ms.Ack()
			}
		}
		writerMutex.Lock()
		delete(timeKeyWriters, timeKey)
		writerMutex.Unlock()
		close(ch)
		logger.Info("job: ", job.Name, " event_type: ", k, " time key: ", timeKey, " uid: ", uid, " writer stopped")
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
		logger.Fatalln(fmt.Errorf("google storage client client error: %v", err))
	}
	ignored := uint64(0)

	// Create a map of event tables to insert into
	eventsToIngest := make(map[string]bool)
	messagesToIngest := make(map[string]chan *pubsub.Message)
	buffer := int(float64(job.Destination.BatchSize) * float64(1.25))
	bulkSize := job.Destination.BatchSize
	logger.Info("google storage sync started for task: ", job.Name, " with bulkSize: ", bulkSize, " with buffer: ", buffer)

	for _, filter := range job.Filters {
		if filter.Action == "ingest" {
			messagesToIngest[filter.Name] = make(chan *pubsub.Message, buffer)
			it := 0
			go func(k string) {
				for mx := range messagesToIngest[k] {
					var event map[string]interface{}
					err := json.Unmarshal(mx.Data, &event)
					if err != nil {
						mx.Nack()
						logger.Fatalln(fmt.Errorf("%s.Put: %v", k, err))
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
					logger.Errorln("attributes: ", msg.Attributes, ", event: ", xtype, ", payload: ", string(msg.Data))
					logger.Errorln(fmt.Sprintf("event type %s not found", xtype))
				}
			}
		}
	}()
}
