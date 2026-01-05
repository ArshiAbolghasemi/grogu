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
	ticker := time.NewTicker(time.Duration(config.Conf.DeadLetterCallInterval) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dlWorker.processDeadLetterCalls(ctx)
		}
	}
}

func (dlWorker *DeadLetterWorker) processDeadLetterCalls(ctx context.Context) {
	dlCalls, err := dlWorker.DLRepository.GetPendingCalls(ctx)
	if err != nil {
		return
	}

	if len(dlCalls) == 0 {
		logging.Logger.Info("no dl calls are existed")
		return
	}

	logging.Logger.Info("start processing dl calls", zap.Int("count_dl_calls", len(dlCalls)))

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
