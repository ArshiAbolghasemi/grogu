package kafka

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Consumer struct {
	Client sarama.ConsumerGroup
}

// NewConsumer creates and returns a new Kafka consumer instance using the provided configuration.
func NewConsumer() (*Consumer, error) {
	client, err := createConsumerGroup(config.Conf.KafkaGroupID, "Telc")
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Client: client,
	}, nil
}

// Consume starts consuming messages from the configured Kafka topic.
func (c *Consumer) Consume(
	ctx context.Context,
	topic string,
	messageHandler func(context.Context, *sarama.ConsumerMessage),
) error {
	handler := &consumerGroupHandler{
		messageHandler: messageHandler,
	}

	runConsumerLoop(ctx, c.Client, topic, handler, "Telc")

	return nil
}

func (c *Consumer) Close() error {
	err := c.Client.Close()
	if err != nil {
		logging.Logger.Error("Failed to close Kafka producer", zap.String("error", err.Error()))
		return err
	}

	logging.Logger.Info("Kafka producer closed successfully")

	return nil
}

type consumerGroupHandler struct {
	messageHandler func(context.Context, *sarama.ConsumerMessage)
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			h.messageHandler(session.Context(), message)

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
