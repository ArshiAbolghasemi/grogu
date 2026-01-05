package grogu

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/asr"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/call"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/deadletter"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/healthchecker"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/kafka"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/minio"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Grogu struct {
	DBConn               *gorm.DB
	MinioClient          *minio.MinioClient
	KafkaConsumer        *kafka.Consumer
	KafkaIvaConsumer     *kafka.IvaConsumer
	KafkaProducer        *kafka.Producer
	WorkerPool           *ants.Pool
	CallService          *call.CallService
	DeadLetterService    *deadletter.DeadLetterService
	DeadLetterWorker     *deadletter.DeadLetterWorker
	HealthCheckerService *healthchecker.Healthchecker
	SourcePlatform       string
}

func NewApp(ctxCancelFun context.CancelFunc) (*Grogu, error) {
	healthcheckerService := healthchecker.NewService(ctxCancelFun)

	dbConn, err := database.NewDatabase()
	if err != nil {
		return nil, err
	}

	minioClient, err := minio.NewMinioClient()
	if err != nil {
		return nil, err
	}

	// Initialize Kafka consumers based on SourcePlatform
	var kafkaConsumer *kafka.Consumer

	var kafkaIvaConsumer *kafka.IvaConsumer

	sourcePlatform := config.Conf.SourcePlatform
	logging.Logger.Info("Initializing Kafka consumers for platform", zap.String("platform", sourcePlatform))

	switch sourcePlatform {
	case "Telc":
		kafkaConsumer, err = kafka.NewConsumer()
		if err != nil {
			return nil, err
		}
	case "iva":
		kafkaIvaConsumer, err = kafka.NewIvaConsumer()
		if err != nil {
			return nil, err
		}
	}

	kafkaProducer, err := kafka.NewProducer()
	if err != nil {
		return nil, err
	}

	workerPool, err := ants.NewPool(config.Conf.PoolSize, ants.WithPreAlloc(true))
	if err != nil {
		return nil, err
	}

	asrClient := asr.NewClient()

	callService := call.NewService(dbConn, minioClient, asrClient)

	deadletterService := deadletter.NewService(dbConn, kafkaProducer, callService)

	deadletterWorker, err := deadletter.NewWorker(deadletterService, dbConn)
	if err != nil {
		return nil, err
	}

	circuitbreak.Init()

	return &Grogu{
		DBConn:               dbConn,
		MinioClient:          minioClient,
		KafkaConsumer:        kafkaConsumer,
		KafkaIvaConsumer:     kafkaIvaConsumer,
		KafkaProducer:        kafkaProducer,
		WorkerPool:           workerPool,
		CallService:          callService,
		DeadLetterService:    deadletterService,
		DeadLetterWorker:     deadletterWorker,
		HealthCheckerService: healthcheckerService,
		SourcePlatform:       sourcePlatform,
	}, nil
}

func (app *Grogu) Run(ctx context.Context) error {
	go app.HealthCheckerService.Monitor()
	go app.DeadLetterWorker.Run(ctx)

	var err error

	// Choose the appropriate Kafka consumer and message handler based on SourcePlatform
	switch app.SourcePlatform {
	case "Telc":
		logging.Logger.Info("Starting Telc Kafka consumer",
			zap.String("topic", config.Conf.KafkaASRTopic),
		)

		err = app.KafkaConsumer.Consume(ctx, config.Conf.KafkaASRTopic, app.MessageHandler)
		if err != nil {
			return err
		}

		logging.Logger.Info("Closing Telc Kafka consumer...")

		err = app.KafkaConsumer.Close()
		if err != nil {
			logging.Logger.Error("Failed to close Telc consumer", zap.String("error", err.Error()))
		}
	case "iva":
		logging.Logger.Info("Starting IVA Kafka consumer",
			zap.String("topic", config.Conf.KafkaIvaTopic),
		)

		err = app.KafkaIvaConsumer.Consume(ctx, config.Conf.KafkaIvaTopic, app.IvaMessageHandler)
		if err != nil {
			return err
		}

		logging.Logger.Info("Closing IVA Kafka consumer...")

		err = app.KafkaIvaConsumer.Close()
		if err != nil {
			logging.Logger.Error("Failed to close IVA consumer", zap.String("error", err.Error()))
		}
	}

	logging.Logger.Info("Closing Kafka producer...")

	err = app.KafkaProducer.Close()
	if err != nil {
		logging.Logger.Error("Failed to close producer", zap.String("error", err.Error()))
	}

	logging.Logger.Info("App shutdown complete")

	return nil
}
