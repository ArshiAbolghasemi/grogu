package healthchecker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	prometheusGrogu "git.mci.dev/mse/sre/phoenix/golang/grogu/internal/prometheus"
	"go.uber.org/zap"
)

var ErrUnknownService = errors.New("unknown service")

type Healthchecker struct {
	CtxCancelFunc context.CancelFunc
	ErrorService  string
}

func NewService(ctxCancelFunc context.CancelFunc) *Healthchecker {
	return &Healthchecker{
		CtxCancelFunc: ctxCancelFunc,
	}
}

func (h *Healthchecker) Monitor() {
	logging.Logger.Info("[Monitor] Health checker monitor started, waiting for circuit break events...")

	serviceName := <-circuitbreak.CircuitBreakChan

	logging.Logger.Error("[Monitor] !!!!! CIRCUIT BREAKER TRIGGERED !!!!!",
		zap.String("service", serviceName),
	)

	prometheusGrogu.CircuitBreakerStatus.WithLabelValues(serviceName).Set(0)
	prometheusGrogu.CircuitBreakerTransitionsTotal.WithLabelValues(serviceName, "broken").Inc()

	h.ErrorService = serviceName

	logging.Logger.Warn("[Monitor] Calling context cancel function to shutdown app...")
	h.CtxCancelFunc()
	logging.Logger.Warn("[Monitor] Context cancel function called, app should begin shutting down")
}

func (h *Healthchecker) Check() {
	logging.Logger.Info("[Check] Starting health check recovery process...",
		zap.String("error_service", h.ErrorService),
	)

	if h.ErrorService == "" {
		logging.Logger.Error("[Check] ERROR: healthchecker error service is empty, cannot perform health check")
		return
	}

	interval := time.Duration(config.Conf.HealthCheckerMonitorInterval) * time.Second
	logging.Logger.Info("[Check] Will check service health every interval",
		zap.String("service", h.ErrorService),
		zap.Duration("interval", interval),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	checkCount := 0

	for {
		<-ticker.C

		checkCount++

		logging.Logger.Info("[Check] Performing health check attempt",
			zap.String("service", h.ErrorService),
			zap.Int("attempt", checkCount),
		)

		err := h.checkErrorService()
		if err == nil {
			logging.Logger.Info("[Check] ===== SERVICE RECOVERED! =====",
				zap.String("service", h.ErrorService),
				zap.Int("attempts", checkCount),
			)

			prometheusGrogu.CircuitBreakerStatus.WithLabelValues(h.ErrorService).Set(1)
			prometheusGrogu.CircuitBreakerTransitionsTotal.WithLabelValues(h.ErrorService, "healthy").Inc()

			return
		}

		logging.Logger.Warn("[Check] Service still unhealthy, will retry",
			zap.String("service", h.ErrorService),
			zap.Int("attempt", checkCount),
			zap.Error(err),
		)
	}
}

func (h *Healthchecker) checkErrorService() error {
	type checkFunc func() error

	checks := map[string]checkFunc{
		circuitbreak.ASRService:           CheckASR,
		circuitbreak.TelCService:          CheckTelC,
		circuitbreak.DBService:            CheckDB,
		circuitbreak.MinioIVAService:      CheckMinioIVA,
		circuitbreak.MinioCOAService:      CheckMinioCOA,
		circuitbreak.KafkaProducerService: CheckKafkaProducer,
	}

	logging.Logger.Debug("[checkErrorService] Looking up health check function",
		zap.String("service", h.ErrorService),
	)

	check, ok := checks[h.ErrorService]
	if !ok {
		logging.Logger.Warn("[checkErrorService] Unknown service, no health check available",
			zap.String("service", h.ErrorService),
		)

		return fmt.Errorf("%w: %s", ErrUnknownService, h.ErrorService)
	}

	logging.Logger.Info("[checkErrorService] Executing health check function...",
		zap.String("service", h.ErrorService),
	)

	err := check()

	logging.Logger.Info("[checkErrorService] Health check result",
		zap.String("service", h.ErrorService),
		zap.Bool("is_healthy", err == nil),
		zap.Error(err),
	)

	if err == nil {
		logging.Logger.Info("[checkErrorService] Service returned to healthy state!",
			zap.String("service", h.ErrorService),
		)
	}

	return err
}
