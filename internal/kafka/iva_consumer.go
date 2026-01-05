package kafka

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type IvaConsumer struct {
	Client sarama.ConsumerGroup
}

// NewIvaConsumer creates and returns a new Kafka consumer instance for IVA platform
// using the IVA-specific configuration. The consumer is configured with SASL SCRAM-SHA512
func NewIvaConsumer() (*IvaConsumer, error) {
	client, err := createConsumerGroup(config.Conf.KafkaIvaGroupID, "IVA")
	if err != nil {
		return nil, err
	}

	return &IvaConsumer{
		Client: client,
	}, nil
}

// Consume starts consuming messages from the configured IVA Kafka topic.
func (c *IvaConsumer) Consume(
	ctx context.Context,
	topic string,
	messageHandler func(context.Context, *sarama.ConsumerMessage),
) error {
	handler := &consumerGroupHandler{
		messageHandler: messageHandler,
	}

	runConsumerLoop(ctx, c.Client, topic, handler, "IVA")

	return nil
}

func (c *IvaConsumer) Close() error {
	err := c.Client.Close()
	if err != nil {
		logging.Logger.Error("Failed to close IVA Kafka consumer", zap.String("error", err.Error()))
		return err
	}

	logging.Logger.Info("IVA Kafka consumer closed successfully")

	return nil
}
