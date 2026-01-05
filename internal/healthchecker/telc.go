package healthchecker

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/telc"
	"go.uber.org/zap"
)

func CheckTelC() bool {
	telcService := telc.NewService()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := telcService.GetCallInfo(ctx, config.Conf.HealthCheckerSampleCallId)
	isGetInfoOk := (err == nil)
	logging.Logger.Info("telc call info api staus", zap.Bool("ok", isGetInfoOk))

	_, err = telcService.GetVoiceCall(ctx, config.Conf.HealthCheckerSampleCallId)
	isGetFileOk := (err == nil)
	logging.Logger.Info("telc call file api staus", zap.Bool("ok", isGetFileOk))

	return (isGetInfoOk && isGetFileOk)
}
