package grogu

import (
	"context"
	"errors"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/call"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	prometheusGrogu "git.mci.dev/mse/sre/phoenix/golang/grogu/internal/prometheus"
	"github.com/IBM/sarama"
	"github.com/goccy/go-json"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// MessageHandler handles Telc platform messages
func (app *Grogu) MessageHandler(ctx context.Context, msg *sarama.ConsumerMessage) {
	err := app.WorkerPool.Submit(func() {
		app.processTelcMessage(ctx, msg)
	})
	if err != nil {
		logging.Logger.Error("failed to submit job to ants pool", zap.String("error", err.Error()))
	}
}

func (app *Grogu) processTelcMessage(ctx context.Context, msg *sarama.ConsumerMessage) {
	timer := prometheus.NewTimer(
		prometheusGrogu.ProcessMessageDuration.WithLabelValues("Telc"),
	)

	defer func() {
		duration := timer.ObserveDuration()
		logging.Logger.Debug("Process message duration",
			zap.String("platform", "Telc"),
			zap.Duration("duration", duration),
		)
	}()

	// Record Kafka message latency for Telc
	var tempMsg struct {
		CreatedAt string `json:"created_at"`
	}

	_ = json.Unmarshal(msg.Value, &tempMsg)
	app.recordKafkaLatency(tempMsg.CreatedAt, "Telc")

	defer app.handlePanic("Telc")

	result, err := app.CallService.ProcessCallMessage(ctx, msg.Value)
	if err != nil {
		logging.Logger.Error("failed to process call message",
			zap.String("error", err.Error()),
			zap.ByteString("call_id", msg.Key),
			zap.ByteString("msg_value", msg.Value),
		)

		_ = app.DeadLetterService.MarkCall(ctx, string(msg.Key), msg.Value, err.Error())

		return
	}

	app.sendResultToKafka(ctx, msg, result, "Telc")
}

// IvaMessageHandler handles IVA platform messages
func (app *Grogu) IvaMessageHandler(ctx context.Context, msg *sarama.ConsumerMessage) {
	err := app.WorkerPool.Submit(func() {
		app.processIvaMessage(ctx, msg)
	})
	if err != nil {
		logging.Logger.Error("failed to submit IVA job to ants pool", zap.String("error", err.Error()))
	}
}

func (app *Grogu) processIvaMessage(ctx context.Context, msg *sarama.ConsumerMessage) {
	timer := prometheus.NewTimer(
		prometheusGrogu.ProcessMessageDuration.WithLabelValues("IVA"),
	)

	defer func() {
		duration := timer.ObserveDuration()
		logging.Logger.Debug("Process message duration",
			zap.String("platform", "IVA"),
			zap.Duration("duration", duration),
		)
	}()

	var tempMsg struct {
		AgentEndedTime string `json:"agent_ended_time"`
	}

	_ = json.Unmarshal(msg.Value, &tempMsg)
	app.recordKafkaLatency(tempMsg.AgentEndedTime, "IVA")

	defer app.handlePanic("IVA")

	result, err := app.CallService.ProcessIvaCallMessage(ctx, msg.Value)
	if err != nil {
		if app.isSkippedMessage(err) {
			logging.Logger.Debug("Skipped non-session IVA message",
				zap.ByteString("call_id", msg.Key),
			)

			return
		}

		logging.Logger.Error("failed to process IVA call message",
			zap.String("error", err.Error()),
			zap.ByteString("call_id", msg.Key),
			zap.ByteString("msg_value", msg.Value),
		)

		_ = app.DeadLetterService.MarkCall(ctx, string(msg.Key), msg.Value, err.Error())

		return
	}

	app.sendResultToKafka(ctx, msg, result, "IVA")
}

func (app *Grogu) isSkippedMessage(err error) bool {
	return errors.Is(err, call.ErrNotSessionType)
}

func (app *Grogu) sendResultToKafka(
	ctx context.Context,
	msg *sarama.ConsumerMessage,
	result *call.CallResultMessage,
	platform string,
) {
	resultBytes, err := json.Marshal(result)
	if err != nil {
		logging.Logger.Error("failed to marshal call result",
			zap.String("platform", platform),
			zap.String("error", err.Error()),
		)

		_ = app.DeadLetterService.MarkCall(ctx, string(msg.Key), msg.Value, err.Error())

		return
	}

	partition, offset, err := app.KafkaProducer.SendMessage(
		config.Conf.KafkaCallTopic,
		msg.Key,
		resultBytes,
	)
	if err != nil {
		logging.Logger.Error("failed to send result to Kafka",
			zap.String("platform", platform),
			zap.String("error", err.Error()),
			zap.ByteString("call_id", msg.Key),
			zap.ByteString("msg_value", msg.Value),
		)

		_ = app.DeadLetterService.MarkCall(ctx, string(msg.Key), msg.Value, err.Error())

		return
	}

	logging.Logger.Info("message processed successfully",
		zap.String("platform", platform),
		zap.ByteString("call_id", msg.Key),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)
}

func (app *Grogu) recordKafkaLatency(timeStr, platform string) {
	if timeStr == "" {
		return
	}

	var startTime time.Time

	var parseErr error
	// Try RFC3339 first
	startTime, parseErr = time.Parse(time.RFC3339, timeStr)
	if parseErr != nil {
		// Try common SQL format
		startTime, parseErr = time.Parse("2006-01-02 15:04:05", timeStr)
	}

	if parseErr == nil {
		latency := time.Since(startTime).Seconds()
		prometheusGrogu.KafkaMessageLatency.WithLabelValues(platform).Observe(latency)
		logging.Logger.Debug("Kafka message latency",
			zap.String("platform", platform),
			zap.Float64("latency", latency),
		)
	}
}

func (app *Grogu) handlePanic(platform string) {
	if r := recover(); r != nil {
		logging.Logger.Error("panic in message worker",
			zap.String("platform", platform),
			zap.Any("recover", r),
		)
	}
}
