package healthchecker

import (
	"context"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"go.uber.org/zap"
)

type Healthchecker struct {
	CtxCancelFunc context.CancelFunc
	ErrorService  string
}

func NewService(ctxCancelFunc context.CancelFunc) *Healthchecker {
	return &Healthchecker{
		CtxCancelFunc: ctxCancelFunc,
	}
}

func (h *Healthchecker) TriggerError(service string) {
	logging.Logger.Error("service error happened", zap.String("service", service))
	h.ErrorService = service
	h.CtxCancelFunc()
}

func (h *Healthchecker) Monitor() {
	logging.Logger.Info("health checker monitor start successfully")

	serviceName := <-circuitbreak.CircuitBreakChan

	logging.Logger.Info("circuit break happened", zap.String("service", serviceName))
	h.ErrorService = serviceName
	h.CtxCancelFunc()
}

func (h *Healthchecker) Check() {
	if h.ErrorService == "" {
		logging.Logger.Error("healthchecker error server is empty")
	}

	ticker := time.NewTicker(time.Duration(config.Conf.HealthCheckerMonitorInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		ok := h.checkErrorService()
		if ok {
			return
		}
	}
}

func (h *Healthchecker) checkErrorService() bool {
	type checkFunc func() bool

	checks := map[string]checkFunc{
		circuitbreak.ASRService:           CheckASR,
		circuitbreak.TelCService:          CheckTelC,
		circuitbreak.DBService:            CheckDB,
		circuitbreak.MinioService:         CheckMinio,
		circuitbreak.KafkaProducerService: CheckKafkaProducer,
	}

	check, ok := checks[h.ErrorService]
	if !ok {
		logging.Logger.Warn("Unknown service in checkErrorService", zap.String("service", h.ErrorService))
		return false
	}

	isHealthy := check()
	if isHealthy {
		logging.Logger.Info(h.ErrorService + " service back healthy")
	}

	return isHealthy
}
