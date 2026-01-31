package circuitbreak

import (
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/prometheus"
	"go.uber.org/zap"
)

var CircuitBreakChan chan string

const (
	ASRService           = "asr"
	TelCService          = "telc"
	DBService            = "database"
	MinioIVAService      = "minio_iva"
	MinioCOAService      = "minio_coa"
	KafkaProducerService = "kafka_producer"
)

func Init() {
	CircuitBreakChan = make(chan string)

	// Initialize status as healthy for all services
	prometheus.CircuitBreakerStatus.WithLabelValues(ASRService).Set(1)
	prometheus.CircuitBreakerStatus.WithLabelValues(TelCService).Set(1)
	prometheus.CircuitBreakerStatus.WithLabelValues(DBService).Set(1)
	prometheus.CircuitBreakerStatus.WithLabelValues(MinioIVAService).Set(1)
	prometheus.CircuitBreakerStatus.WithLabelValues(MinioCOAService).Set(1)
	prometheus.CircuitBreakerStatus.WithLabelValues(KafkaProducerService).Set(1)
}

func TriggerError(service string) {
	logging.Logger.Error("[TriggerError] Circuit breaker error triggered!",
		zap.String("service", service),
	)

	if CircuitBreakChan == nil {
		logging.Logger.Fatal("[TriggerError] CircuitBreakChan is nil, grogu app is not created")
	}

	logging.Logger.Debug("[TriggerError] Sending service name to CircuitBreakChan...",
		zap.String("service", service),
	)

	CircuitBreakChan <- service

	logging.Logger.Debug("[TriggerError] Service name sent to CircuitBreakChan",
		zap.String("service", service),
	)
}
