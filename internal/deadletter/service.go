package deadletter

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/call"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/kafka"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type DeadLetterService struct {
	DLRepository  *DeadLetterRepository
	CallService   *call.CallService
	KafkaProducer *kafka.Producer
}

func NewService(dbConn *gorm.DB, kafkaProducer *kafka.Producer, callService *call.CallService) *DeadLetterService {
	dlRepository := NewRepository(dbConn)

	return &DeadLetterService{
		DLRepository:  dlRepository,
		CallService:   callService,
		KafkaProducer: kafkaProducer,
	}
}

func (dlService *DeadLetterService) MarkCall(ctx context.Context, callID string, msg []byte, errMsg string) error {
	_, err := dlService.DLRepository.CreateCall(ctx, callID, msg, errMsg)
	if err != nil {
		return err
	}

	logging.Logger.Info("mark call as dead letter", zap.String("call_id", callID))

	return nil
}

func (dlService *DeadLetterService) ProcessDeadLetterCall(ctx context.Context, dlCall *CallASRDeadLetter) {
	err := dlService.DLRepository.UpdateDLCallStatusToInprogress(ctx, dlCall)
	if err != nil {
		logging.Logger.Info("failed to update dlCall to in progress", zap.String("call_id", dlCall.CallID))
		return
	}

	logging.Logger.Info("dl call status updated successfully", zap.String("call_id", dlCall.CallID))

	result, err := dlService.CallService.ProcessCallMessage(ctx, dlCall.Msg)
	if err != nil {
		logging.Logger.Error("failed to reprocess message",
			zap.String("call_id", dlCall.CallID),
			zap.String("error", err.Error()),
		)
		_ = dlService.DLRepository.IncreaseRetryCount(ctx, dlCall, err.Error())

		return
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		logging.Logger.Error("failed to marshal call result", zap.String("error", err.Error()))
		_ = dlService.DLRepository.IncreaseRetryCount(ctx, dlCall, err.Error())

		return
	}

	partition, offset, err := dlService.KafkaProducer.SendMessage(
		config.Conf.KafkaCallTopic,
		[]byte(dlCall.CallID),
		resultBytes,
	)
	if err != nil {
		logging.Logger.Error("failed to send result dl call to Kafka",
			zap.String("error", err.Error()),
			zap.String("call_id", dlCall.CallID),
			zap.ByteString("msg_value", dlCall.Msg),
		)
		_ = dlService.DLRepository.IncreaseRetryCount(ctx, dlCall, err.Error())

		return
	}

	logging.Logger.Info("dl call processed successfully",
		zap.String("call_id", dlCall.CallID),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	err = dlService.DLRepository.DeleteDlCallRecord(ctx, dlCall)
	if err != nil {
		logging.Logger.Info("failed to delete processed dl call",
			zap.String("call_id", dlCall.CallID),
			zap.String("error", err.Error()),
		)
	}
}
