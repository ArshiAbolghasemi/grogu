package logging

import (
	"os"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging/zapconsole"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

func init() {
	var err error

	Logger, err = getDoubleLogger()
	if err != nil {
		zap.NewExample().Fatal("Could not initialize logger", zap.String("error", err.Error()))
	}
}

func getDoubleLogger() (*zap.Logger, error) {
	productionEncoderConfig := zap.NewProductionEncoderConfig()
	productionEncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	developmentEncoderConfig := zap.NewDevelopmentEncoderConfig()
	developmentEncoderConfig.ConsoleSeparator = "  "

	level, err := zapcore.ParseLevel(config.Conf.LogLevel)
	if err != nil {
		zap.NewExample().Info("Invalid log level, using info level")

		level = zapcore.InfoLevel
	}

	zapConfig := &zap.Config{
		Level:             zap.NewAtomicLevelAt(level),
		Development:       false,
		DisableCaller:     false,
		DisableStacktrace: false,
		Encoding:          "json",
		EncoderConfig:     productionEncoderConfig,
		OutputPaths:       []string{config.Conf.LogFilePath},
	}

	fileLogger, err := zapConfig.Build()
	if err != nil {
		return nil, err
	}

	consoleEncoder := zapconsole.NewConsoleEncoder(&developmentEncoderConfig)

	core := zapcore.NewTee(
		fileLogger.Core(),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)

	return zap.New(core, zap.AddCaller()), nil
}
