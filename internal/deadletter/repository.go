package deadletter

import (
	"context"
	"errors"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	ErrInvalidCallASRDeadLetterResult      = errors.New("invalid result type, it should be pointer to CallASRDeadLetter")
	ErrInvalidCallASRDeadLetterSliceResult = errors.New("invalid result type, it should be slice of CallASRDeadLetter")
)

type DeadLetterRepository struct {
	DBConn         *gorm.DB
	CircuitBreaker *gobreaker.CircuitBreaker[any]
}

func NewRepository(dbConn *gorm.DB) *DeadLetterRepository {
	cbSettings := database.GetCircuitBreakerSettings()

	return &DeadLetterRepository{
		DBConn:         dbConn,
		CircuitBreaker: gobreaker.NewCircuitBreaker[any](cbSettings),
	}
}

func (dlRepository *DeadLetterRepository) CreateCall(
	ctx context.Context,
	callID string,
	msg []byte,
	errMsg string,
) (*CallASRDeadLetter, error) {
	result, err := dlRepository.CircuitBreaker.Execute(func() (any, error) {
		now := time.Now()
		dlCall := CallASRDeadLetter{
			CallID:      callID,
			Msg:         msg,
			Error:       errMsg,
			Status:      StatusPending,
			LastRetryAt: &now,
		}

		var (
			err    error
			dbConn *gorm.DB
		)

		select {
		case <-ctx.Done():
			dbConn = dlRepository.DBConn
		default:
			dbConn = dlRepository.DBConn.WithContext(ctx)
		}

		// Use upsert to handle duplicate call_id (updates existing record if exists)
		err = dbConn.Where("call_id = ?", callID).
			Assign(map[string]interface{}{
				"msg":           msg,
				"error":         errMsg,
				"status":        StatusPending,
				"last_retry_at": &now,
			}).
			FirstOrCreate(&dlCall).Error
		if err != nil {
			logging.Logger.Error("failed to create dead letter record",
				zap.String("call_id", callID),
				zap.String("error", err.Error()),
			)

			return nil, err
		}

		return &dlCall, nil
	})
	if err != nil {
		return nil, err
	}

	dlCall, ok := result.(*CallASRDeadLetter)
	if !ok {
		return nil, ErrInvalidCallASRDeadLetterResult
	}

	return dlCall, nil
}

func (dlRepository *DeadLetterRepository) GetPendingCalls(ctx context.Context) ([]CallASRDeadLetter, error) {
	result, err := dlRepository.CircuitBreaker.Execute(func() (any, error) {
		var records []CallASRDeadLetter

		err := dlRepository.DBConn.WithContext(ctx).
			Where(
				"status = ? AND last_retry_at <= ? AND retry_count < ?",
				StatusPending,
				time.Now().Add(-time.Duration(config.Conf.DeadLetterCallRetryDelay)*time.Hour),
				config.Conf.DeadLetterCallMaxRetries,
			).
			Order("created_at ASC").
			Limit(config.Conf.DeadLetterCallMaxRetries).
			Find(&records).Error
		if err != nil {
			logging.Logger.Info("failed to fetch dead letter calls", zap.String("error", err.Error()))
			return nil, err
		}

		return records, err
	})
	if err != nil {
		return nil, err
	}

	records, ok := result.([]CallASRDeadLetter)
	if !ok {
		return nil, ErrInvalidCallASRDeadLetterSliceResult
	}

	return records, nil
}

func (dlRepository *DeadLetterRepository) UpdateCallStatus(
	ctx context.Context,
	dlCall *CallASRDeadLetter,
	status string,
) error {
	_, err := dlRepository.CircuitBreaker.Execute(func() (any, error) {
		err := dlRepository.DBConn.
			WithContext(ctx).
			Model(dlCall).
			Where("call_id = ?", dlCall.CallID).
			Update("status", status).Error
		if err != nil {
			return nil, err
		}

		return dlCall, nil
	})

	return err
}

func (dlRepository *DeadLetterRepository) IncreaseRetryCount(
	ctx context.Context,
	dlCall *CallASRDeadLetter,
	errMsg string,
) error {
	_, err := dlRepository.CircuitBreaker.Execute(func() (any, error) {
		updates := map[string]any{
			"retry_count":   gorm.Expr("retry_count + 1"),
			"last_retry_at": time.Now(),
			"status":        StatusPending,
			"error":         errMsg,
		}

		err := dlRepository.DBConn.WithContext(ctx).
			Model(dlCall).
			Where("call_id = ?", dlCall.CallID).
			Updates(updates).Error
		if err != nil {
			logging.Logger.Error("failed to increase dl call retry count",
				zap.String("call_id", dlCall.CallID),
				zap.String("error", err.Error()),
			)

			return nil, err
		}

		return dlCall, nil
	})

	return err
}

func (dlRepository *DeadLetterRepository) DeleteDlCallRecord(ctx context.Context, dlCall *CallASRDeadLetter) error {
	_, err := dlRepository.CircuitBreaker.Execute(func() (any, error) {
		err := dlRepository.DBConn.WithContext(ctx).
			Where("call_id = ?", dlCall.CallID).
			Delete(dlCall).
			Error

		return nil, err
	})

	return err
}
