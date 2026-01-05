package prometheus

import (
	"errors"
	"net/http"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func Run() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	addr := ":" + config.Conf.PrometheusPort

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       time.Duration(config.Conf.PrometheusTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(config.Conf.PrometheusTimeout) * time.Second,
		WriteTimeout:      time.Duration(config.Conf.PrometheusTimeout) * time.Second,
		IdleTimeout:       time.Duration(config.Conf.PrometheusTimeout) * time.Second,
	}

	logging.Logger.Info("start prometheus server on port " + config.Conf.PrometheusPort)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logging.Logger.Error(
			"failed to start prometheus server",
			zap.String("error", err.Error()),
		)
	}
}
