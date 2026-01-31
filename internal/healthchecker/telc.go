package healthchecker

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/telc"
	"go.uber.org/zap"
)

func CheckTelC() error {
	telcService := telc.NewService()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := telcService.GetCallInfo(ctx, config.Conf.HealthCheckerSampleCallId)
	if err != nil {
		logging.Logger.Info("telc call info api status", zap.Error(err))
		return err
	}

	_, err = telcService.GetVoiceCall(ctx, config.Conf.HealthCheckerSampleCallId)
	if err != nil {
		logging.Logger.Info("telc call file api status", zap.Error(err))
		return err
	}

	return nil
}
