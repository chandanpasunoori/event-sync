package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/chandanpasunoori/event-sync/pkg"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

const (
	version = "0.0.7"
)

var verbose bool
var configDoc string
var config pkg.Config

var rootCmd = &cobra.Command{
	Use:     "event-sync",
	Short:   "Event Sync is for syncing events from multiple sources to multiple destinations",
	Long:    `Built to ease process of syncing data between different storage systems`,
	Version: version,
	Run: func(cmd *cobra.Command, args []string) {

		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
		zerolog.SetGlobalLevel(zerolog.DebugLevel)

		if verbose {
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
		}
		configBytes, err := os.ReadFile(configDoc)
		if err != nil {
			log.Error().Err(err).Str("path", configDoc).Msg("config file not found")
			os.Exit(1)
		}
		if err := json.Unmarshal(configBytes, &config); err != nil {
			log.Error().Err(err).Msg("error parsing config")
			os.Exit(1)
		}
		log.Info().Str("version", version).Msg("event-sync is ready to sync events")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		delay := 15 * time.Second
		go func() {
			<-signalChan
			cancel()
			log.Info().Str("version", version).Dur("delay", delay).Msgf("program interupted, waiting for %s", delay)
			<-time.NewTimer(delay).C
			os.Exit(0)
		}()
		go pkg.SyncEvents(ctx, config)
		runServer(ctx)
	},
}

func runServer(ctx context.Context) {
	http.Handle("/metrics", promhttp.Handler())
	healthCheckHandler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(200)
		json.NewEncoder(rw).Encode(map[string]string{"status": "ok"})
	})

	http.Handle("/", healthCheckHandler)
	http.Handle("/_status/healthz", healthCheckHandler)

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Warn().Err(err).Msg("http server error")
	}
	log.Info().Msg("server stopped")
}

func Execute() {
	if genDoc := os.Getenv("GEN_DOC"); genDoc == "true" {
		err := doc.GenMarkdownTree(rootCmd, "./docs")
		if err != nil {
			log.Error().Err(err).Msg("failed generating docs")
		}
	}

	if err := rootCmd.Execute(); err != nil {
		log.Error().Err(err).Msg("error executing command")
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose mode")
	rootCmd.PersistentFlags().StringVarP(&configDoc, "config", "c", "app.json", "job configuration file path")
	_ = rootCmd.MarkFlagRequired("config")
}
