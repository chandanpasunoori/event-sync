package pkg

import (
	"context"
	"errors"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog/log"
)

func SyncEvents(ctx context.Context, config Config) {
	var wg sync.WaitGroup
	for _, job := range config.Jobs {
		if !job.Suspend {
			var eventChannel chan *pubsub.Message
			if job.Source.Type == "google-pubsub" {
				eventChannel = SubscribePubsubAndPull(ctx, &wg, job)
				switch job.Destination.Type {
				case "bigquery":
					WaitAndBQSync(ctx, &wg, job, eventChannel)
				case "google-storage":
					WaitAndGoogleStorageSync(ctx, &wg, job, eventChannel, false)
				case "google-storage-load":
					WaitAndGoogleStorageSync(ctx, &wg, job, eventChannel, true)
				default:
					log.Fatal().Err(errors.New("invalid job destination type")).Msg("invalid config")
				}
			}
		}
	}
	wg.Wait()
}
