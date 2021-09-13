package pkg

import (
	"sync"
)

func SyncEvents(config Config) {
	var wg sync.WaitGroup
	for _, job := range config.Jobs {
		eventChannel := SubscribePubsubAndPull(&wg, job)
		WaitAndBQSync(&wg, job, eventChannel)
	}
	wg.Wait()
}
