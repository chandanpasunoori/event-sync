package pkg

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
