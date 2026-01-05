package circuitbreak

import "git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"

var CircuitBreakChan chan string

const (
	ASRService           = "asr"
	TelCService          = "telc"
	DBService            = "database"
	MinioService         = "minio"
	KafkaProducerService = "kafka_producer"
)

func Init() {
	CircuitBreakChan = make(chan string)
}

func TriggerError(service string) {
	if CircuitBreakChan == nil {
		logging.Logger.Fatal("grogu app is not created")
	}

	CircuitBreakChan <- service
}
