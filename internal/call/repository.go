package call

import (
	"context"
	"errors"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var ErrInvalidTelcCallRecordResult = errors.New("invalid result type, it should be pointer to telcCallRecord struct")

type CallRepository struct {
	DBConn         *gorm.DB
	CircuitBreaker *gobreaker.CircuitBreaker[any]
}

func NewCallRepository(dbConn *gorm.DB) *CallRepository {
	cbSettings := database.GetCircuitBreakerSettings()

	return &CallRepository{
		DBConn:         dbConn,
		CircuitBreaker: gobreaker.NewCircuitBreaker[any](cbSettings),
	}
}

// UpdateCallStatus updates the status of a Call identified by callID.
func (callRepository *CallRepository) UpdateCallStatus(ctx context.Context, callID, status string) error {
	_, err := callRepository.CircuitBreaker.Execute(func() (any, error) {
		var call Call

		err := callRepository.DBConn.WithContext(ctx).
			Where("call_id = ?", callID).
			First(&call).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return callRepository.createCall(ctx, callID, status)
		}

		if err != nil {
			logging.Logger.Error("failed to fetch call",
				zap.String("call_id", callID),
				zap.String("error", err.Error()),
			)

			return nil, err
		}

		if call.Status == status {
			return &call, nil
		}

		err = callRepository.DBConn.WithContext(ctx).
			Model(&call).
			Where("call_id = ?", callID).
			Update("status", status).Error
		if err != nil {
			return nil, err
		}

		return &call, nil
	})

	return err
}

// GetTelcCallRecordByID retrieves a TelCCallRecord by its callID.
func (callRepository *CallRepository) GetTelcCallRecordByID(
	ctx context.Context,
	callID string,
) (*TelCCallRecord, error) {
	result, err := callRepository.CircuitBreaker.Execute(func() (any, error) {
		var telcCallRecord TelCCallRecord

		err := callRepository.DBConn.WithContext(ctx).
			Where("call_id = ?", callID).
			First(&telcCallRecord).Error
		if err != nil {
			logging.Logger.Error("failed to fetch call record",
				zap.String("call_id", callID),
				zap.String("error", err.Error()),
			)

			return nil, err
		}

		return &telcCallRecord, nil
	})
	if err != nil {
		return nil, err
	}

	telcCallRecord, ok := result.(*TelCCallRecord)
	if !ok {
		return nil, ErrInvalidTelcCallRecordResult
	}

	return telcCallRecord, nil
}

// UpdateTelCCallRecord updates the Reasons, CallStartDate, and CallEndDate
func (callRepository *CallRepository) UpdateTelCCallRecord(
	ctx context.Context,
	record *TelCCallRecord,
	reasons []byte,
	startTime, endTime *time.Time,
) error {
	updates := buildTelCCallUpdates(reasons, startTime, endTime)
	if len(updates) == 0 {
		return nil
	}

	_, err := callRepository.CircuitBreaker.Execute(func() (any, error) {
		err := callRepository.DBConn.
			WithContext(ctx).
			Model(record).
			Where("call_id = ?", record.CallID).
			Updates(updates).Error
		if err != nil {
			return nil, err
		}

		return record, nil
	})

	return err
}

func buildTelCCallUpdates(reasons []byte, startTime, endTime *time.Time) map[string]any {
	updates := make(map[string]any)

	if reasons != nil {
		updates["reasons"] = reasons
	}

	if startTime != nil {
		updates["call_start_date"] = startTime
	}

	if endTime != nil {
		updates["call_end_date"] = endTime
	}

	return updates
}

func (callRepository *CallRepository) createCall(ctx context.Context, callID, status string) (*Call, error) {
	call := Call{
		CallID: callID,
		Status: status,
	}

	err := callRepository.DBConn.WithContext(ctx).Create(&call).Error
	if err != nil {
		logging.Logger.Error("failed to create call",
			zap.String("call_id", callID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	logging.Logger.Info("Call did not exist, so it was created.", zap.String("call_id", callID))

	return &call, err
}
