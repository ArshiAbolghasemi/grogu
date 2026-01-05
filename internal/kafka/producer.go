package kafka

import (
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/IBM/sarama"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
)

type ProducerResult struct {
	Partition int32
	Offset    int64
}

type Producer struct {
	Client         sarama.SyncProducer
	CircuitBreaker *gobreaker.CircuitBreaker[ProducerResult]
}

// NewProducer creates and returns a new Kafka producer instance using the provided configuration.
func NewProducer() (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	cfg.Net.SASL.User = config.Conf.KafkaUsername
	cfg.Net.SASL.Password = config.Conf.KafkaPassword
	cfg.Net.SASL.Handshake = true
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &XDGSCRAMClient{}
	}
	cfg.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer([]string{config.Conf.KafkaBootstrapServer}, cfg)
	if err != nil {
		logging.Logger.Error("Failed to create Kafka producer",
			zap.String("bootstrap", config.Conf.KafkaBootstrapServer),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	logging.Logger.Info("Successfully connected to Kafka producer",
		zap.String("bootstrap", config.Conf.KafkaBootstrapServer),
		zap.String("mechanism", "SCRAM-SHA-512"),
	)

	return &Producer{
		Client:         client,
		CircuitBreaker: newKafkaProducerCircuitBreaker(),
	}, nil
}

func newKafkaProducerCircuitBreaker() *gobreaker.CircuitBreaker[ProducerResult] {
	settings := gobreaker.Settings{
		Name:     "KafkaProducer",
		Interval: time.Duration(config.Conf.KafkaIntervalCB) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= config.Conf.KafkaConsecutiveFailuresCB
		},
		OnStateChange: func(name string, fromState, toState gobreaker.State) {
			logging.Logger.Warn("Circuit state changed",
				zap.String("service", name),
				zap.String("from", fromState.String()),
				zap.String("to", toState.String()),
			)

			if toState == gobreaker.StateOpen {
				circuitbreak.TriggerError(circuitbreak.KafkaProducerService)
			}
		},
	}

	return gobreaker.NewCircuitBreaker[ProducerResult](settings)
}

// SendMessage sends a message to the cfgured Kafka topic.
func (p *Producer) SendMessage(topic string, key, value []byte) (int32, int64, error) {
	result, err := p.CircuitBreaker.Execute(func() (ProducerResult, error) {
		return p.doSendMessage(topic, key, value)
	})
	if err != nil {
		return 0, 0, err
	}

	return result.Partition, result.Offset, nil
}

// Close closes the producer and releases all resources.
func (p *Producer) Close() error {
	err := p.Client.Close()
	if err != nil {
		logging.Logger.Error("Failed to close Kafka producer", zap.String("error", err.Error()))
		return err
	}

	logging.Logger.Info("Kafka producer closed successfully")

	return nil
}

func (p *Producer) doSendMessage(topic string, key, value []byte) (ProducerResult, error) {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := p.Client.SendMessage(message)
	if err != nil {
		logging.Logger.Error("Failed to send message to Kafka",
			zap.String("topic", topic),
			zap.String("error", err.Error()),
		)

		return ProducerResult{}, err
	}

	logging.Logger.Debug("Message sent successfully",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return ProducerResult{Partition: partition, Offset: offset}, nil
}
