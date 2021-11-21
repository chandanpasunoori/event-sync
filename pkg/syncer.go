package pkg

import (
	"sync"

	"cloud.google.com/go/pubsub"
)

func SyncEvents(config Config) {
	var wg sync.WaitGroup
	for _, job := range config.Jobs {
		if !job.Suspend {
			var eventChannel chan *pubsub.Message
			if job.Source.Type == "google-pubsub" {
				eventChannel = SubscribePubsubAndPull(&wg, job)
			}
			if job.Destination.Type == "bigquery" {
				WaitAndBQSync(&wg, job, eventChannel)
			}
			if job.Destination.Type == "google-storage" {
				WaitAndGoogleStorageSync(&wg, job, eventChannel)
			}
		}
	}
	wg.Wait()
}
