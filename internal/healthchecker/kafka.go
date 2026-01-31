package healthchecker

import (
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/kafka"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

var healtcheckerMsg = "healthchecker msg"

func CheckKafkaProducer() error {
	kafkaProducer, err := kafka.NewProducer()
	if err != nil {
		logging.Logger.Error("failed to create new kafka producer client", zap.String("error", err.Error()))
		return err
	}

	_, _, err = kafkaProducer.SendMessage(
		config.Conf.KafkaCallTopic,
		[]byte(uuid.New().String()),
		[]byte(healtcheckerMsg),
	)

	return err
}
