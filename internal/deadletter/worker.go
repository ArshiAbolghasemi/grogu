package deadletter

import (
	"context"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type DeadLetterWorker struct {
	WorkerPool   *ants.Pool
	DLService    *DeadLetterService
	DLRepository *DeadLetterRepository
}

func NewWorker(
	dlService *DeadLetterService,
	dbConn *gorm.DB,
) (*DeadLetterWorker, error) {
	workerPool, err := ants.NewPool(config.Conf.DeadLetterPoolSize, ants.WithPreAlloc(true))
	if err != nil {
		return nil, err
	}

	dlRepository := NewRepository(dbConn)

	return &DeadLetterWorker{
		WorkerPool:   workerPool,
		DLService:    dlService,
		DLRepository: dlRepository,
	}, nil
}

func (dlWorker *DeadLetterWorker) Run(ctx context.Context) {
	interval := time.Duration(config.Conf.DeadLetterCallInterval) * time.Minute
	logging.Logger.Info("[DeadLetterWorker.Run] Starting dead letter worker",
		zap.Duration("check_interval", interval),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logging.Logger.Info("[DeadLetterWorker.Run] Context canceled, stopping dead letter worker",
				zap.Error(ctx.Err()),
			)

			return
		case <-ticker.C:
			logging.Logger.Debug("[DeadLetterWorker.Run] Ticker fired, processing dead letter calls")
			dlWorker.processDeadLetterCalls(ctx)
		}
	}
}

func (dlWorker *DeadLetterWorker) processDeadLetterCalls(ctx context.Context) {
	logging.Logger.Debug("[processDeadLetterCalls] Fetching pending dead letter calls...")

	// Check context before processing
	if ctx.Err() != nil {
		logging.Logger.Warn("[processDeadLetterCalls] Context canceled, skipping dead letter processing",
			zap.Error(ctx.Err()),
		)

		return
	}

	dlCalls, err := dlWorker.DLRepository.GetPendingCalls(ctx)
	if err != nil {
		logging.Logger.Error("[processDeadLetterCalls] Failed to get pending calls",
			zap.Error(err),
		)

		return
	}

	if len(dlCalls) == 0 {
		logging.Logger.Debug("[processDeadLetterCalls] No pending dead letter calls")
		return
	}

	logging.Logger.Info("[processDeadLetterCalls] Starting to process dead letter calls",
		zap.Int("count", len(dlCalls)),
	)

	for idx := range dlCalls {
		dlCall := dlCalls[idx]

		err := dlWorker.WorkerPool.Submit(func() {
			dlWorker.DLService.ProcessDeadLetterCall(ctx, &dlCall)
		})
		if err != nil {
			logging.Logger.Error("failed to submit dl worker pool",
				zap.String("call_id", dlCall.CallID),
				zap.String("error", err.Error()),
			)
		}
	}
}
