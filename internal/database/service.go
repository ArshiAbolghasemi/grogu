package database

import (
	"fmt"
	"net/url"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

func NewDatabase() (*gorm.DB, error) {
	dsn := GetDSN()

	gormLoggerInstance := gormLogger.Default.LogMode(gormLogger.Silent)

	database, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormLoggerInstance,
	})
	if err != nil {
		logging.Logger.Error("Failed to connect to Postgres", zap.String("error", err.Error()))
		return nil, err
	}

	sqldatabase, err := database.DB()
	if err != nil {
		logging.Logger.Error("Failed to get sql.database from GORM", zap.String("error", err.Error()))
		return nil, err
	}

	err = sqldatabase.Ping()
	if err != nil {
		logging.Logger.Error("Failed to ping Postgres database", zap.String("error", err.Error()))
		return nil, err
	}

	logging.Logger.Info("Successfully connected to Postgres")

	return database, nil
}

func GetDSN() string {
	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s",
		config.Conf.PostgresHost,
		config.Conf.PostgresUsername,
		config.Conf.PostgresPassword,
		config.Conf.PostgresDatabase,
		config.Conf.PostgresPort,
	)
}

func GetURL() string {
	dbUrl := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(config.Conf.PostgresUsername, config.Conf.PostgresPassword),
		Host:   fmt.Sprintf("%s:%s", config.Conf.PostgresHost, config.Conf.PostgresPort),
		Path:   config.Conf.PostgresDatabase,
	}
	queries := url.Values{}
	queries.Add("sslmode", "disable")
	dbUrl.RawQuery = queries.Encode()

	return dbUrl.String()
}

func GetCircuitBreakerSettings() gobreaker.Settings {
	return gobreaker.Settings{
		Name:     "database",
		Interval: time.Duration(config.Conf.DBIntervalCB) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= config.Conf.DBConsecutiveFailuresCB
		},
		OnStateChange: func(name string, fromSate, toSate gobreaker.State) {
			logging.Logger.Info("Circuit state changed",
				zap.String("service", name),
				zap.String("from", fromSate.String()),
				zap.String("to", toSate.String()),
			)

			if toSate == gobreaker.StateOpen {
				circuitbreak.TriggerError(circuitbreak.DBService)
			}
		},
	}
}
