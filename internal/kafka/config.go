package kafka

import (
	"context"
	"sync"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// newSaramaConfig creates a new Sarama configuration with SCRAM-SHA512 authentication
func newSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_8_0_0

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	cfg.Net.SASL.User = config.Conf.KafkaUsername
	cfg.Net.SASL.Password = config.Conf.KafkaPassword
	cfg.Net.SASL.Handshake = true

	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &XDGSCRAMClient{}
	}

	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.ResetInvalidOffsets = true
	cfg.Consumer.Return.Errors = true

	return cfg
}

// createConsumerGroup creates a new Kafka consumer group with the given group ID and logs the result
func createConsumerGroup(groupID, consumerName string) (sarama.ConsumerGroup, error) {
	cfg := newSaramaConfig()

	client, err := sarama.NewConsumerGroup(
		[]string{config.Conf.KafkaBootstrapServer},
		groupID,
		cfg,
	)
	if err != nil {
		logging.Logger.Error("Failed to create Kafka consumer group",
			zap.String("consumer", consumerName),
			zap.String("bootstrap", config.Conf.KafkaBootstrapServer),
			zap.String("group_id", groupID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	logging.Logger.Info("Successfully connected to Kafka",
		zap.String("consumer", consumerName),
		zap.String("bootstrap", config.Conf.KafkaBootstrapServer),
		zap.String("group_id", groupID),
		zap.String("mechanism", "SCRAM-SHA-512"),
	)

	return client, nil
}

// runConsumerLoop runs the consumer group loop with the given handler
func runConsumerLoop(
	ctx context.Context,
	client sarama.ConsumerGroup,
	topic string,
	handler *consumerGroupHandler,
	consumerName string,
) {
	var waitGroup sync.WaitGroup

	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()

		topics := []string{topic}

		for {
			err := client.Consume(ctx, topics, handler)
			if err != nil {
				logging.Logger.Error("Kafka consume error",
					zap.String("consumer", consumerName),
					zap.String("error", err.Error()),
				)
			}

			if ctx.Err() != nil {
				logging.Logger.Info("Kafka consumer stopping (context canceled)",
					zap.String("consumer", consumerName),
					zap.String("error", ctx.Err().Error()),
				)

				return
			}
		}
	}()

	go func() {
		for err := range client.Errors() {
			logging.Logger.Error("Kafka consumer internal error",
				zap.String("consumer", consumerName),
				zap.String("error", err.Error()),
			)
		}
	}()

	waitGroup.Wait()
}
