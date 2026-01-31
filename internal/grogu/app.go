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
	MinioIVAClient       *minio.MinioClient
	MinioCOAClient       *minio.MinioClient
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
	logging.Logger.Info("[NewApp] Initializing Grogu application...")

	healthcheckerService := healthchecker.NewService(ctxCancelFun)

	logging.Logger.Info("[NewApp] Health checker service created")

	dbConn, err := database.NewDatabase()
	if err != nil {
		logging.Logger.Error("[NewApp] Failed to initialize database", zap.Error(err))
		return nil, err
	}

	logging.Logger.Info("[NewApp] Database connection established")

	minioIVAClient, err := minio.NewMinioClient(
		config.Conf.MinioIvaAccessKey,
		config.Conf.MinioIvaSecretKey,
		config.Conf.MinioIvaBucketName,
		config.Conf.MinioIvaPathPrefix,
		circuitbreak.MinioIVAService,
	)
	if err != nil {
		logging.Logger.Error("[NewApp] Failed to initialize Minio IVA client", zap.Error(err))
		return nil, err
	}

	minioCOAClient, err := minio.NewMinioClient(
		config.Conf.MinioAccessKey,
		config.Conf.MinioSecretKey,
		config.Conf.MinioBucketName,
		config.Conf.MinioPathPrefix,
		circuitbreak.MinioCOAService,
	)
	if err != nil {
		logging.Logger.Error("[NewApp] Failed to initialize Minio COA client", zap.Error(err))
		return nil, err
	}

	logging.Logger.Info("[NewApp] Minio client created")

	kafkaConsumer, kafkaIvaConsumer, err := initializeKafkaConsumers()
	if err != nil {
		return nil, err
	}

	kafkaProducer, workerPool, err := initializeKafkaProducerAndPool()
	if err != nil {
		return nil, err
	}

	_, callService, deadletterService, deadletterWorker, err := initializeServices(
		dbConn,
		minioIVAClient,
		minioCOAClient,
		kafkaProducer,
	)
	if err != nil {
		return nil, err
	}

	logging.Logger.Info("[NewApp] Initializing circuit breakers...")
	circuitbreak.Init()
	logging.Logger.Info("[NewApp] Circuit breakers initialized")

	return &Grogu{
		DBConn:               dbConn,
		MinioIVAClient:       minioIVAClient,
		MinioCOAClient:       minioCOAClient,
		KafkaConsumer:        kafkaConsumer,
		KafkaIvaConsumer:     kafkaIvaConsumer,
		KafkaProducer:        kafkaProducer,
		WorkerPool:           workerPool,
		CallService:          callService,
		DeadLetterService:    deadletterService,
		DeadLetterWorker:     deadletterWorker,
		HealthCheckerService: healthcheckerService,
		SourcePlatform:       config.Conf.SourcePlatform,
	}, nil
}

func initializeKafkaConsumers() (*kafka.Consumer, *kafka.IvaConsumer, error) {
	var (
		kafkaConsumer    *kafka.Consumer
		kafkaIvaConsumer *kafka.IvaConsumer
	)

	sourcePlatform := config.Conf.SourcePlatform
	logging.Logger.Info("[NewApp] Initializing Kafka consumers for platform", zap.String("platform", sourcePlatform))

	switch sourcePlatform {
	case "Telc":
		logging.Logger.Info("[NewApp] Creating Telc Kafka consumer...")

		var err error

		kafkaConsumer, err = kafka.NewConsumer()
		if err != nil {
			logging.Logger.Error("[NewApp] Failed to create Telc Kafka consumer", zap.Error(err))
			return nil, nil, err
		}

		logging.Logger.Info("[NewApp] Telc Kafka consumer created")
	case "iva":
		logging.Logger.Info("[NewApp] Creating IVA Kafka consumer...")

		var err error

		kafkaIvaConsumer, err = kafka.NewIvaConsumer()
		if err != nil {
			logging.Logger.Error("[NewApp] Failed to create IVA Kafka consumer", zap.Error(err))
			return nil, nil, err
		}

		logging.Logger.Info("[NewApp] IVA Kafka consumer created")
	}

	return kafkaConsumer, kafkaIvaConsumer, nil
}

func initializeKafkaProducerAndPool() (*kafka.Producer, *ants.Pool, error) {
	logging.Logger.Info("[NewApp] Creating Kafka producer...")

	kafkaProducer, err := kafka.NewProducer()
	if err != nil {
		logging.Logger.Error("[NewApp] Failed to create Kafka producer", zap.Error(err))
		return nil, nil, err
	}

	logging.Logger.Info("[NewApp] Kafka producer created")

	logging.Logger.Info("[NewApp] Creating worker pool",
		zap.Int("pool_size", config.Conf.PoolSize),
	)

	workerPool, err := ants.NewPool(config.Conf.PoolSize, ants.WithPreAlloc(true))
	if err != nil {
		logging.Logger.Error("[NewApp] Failed to create worker pool", zap.Error(err))
		return nil, nil, err
	}

	logging.Logger.Info("[NewApp] Worker pool created successfully")

	return kafkaProducer, workerPool, nil
}

func initializeServices(
	dbConn *gorm.DB,
	minioIVAClient *minio.MinioClient,
	minioCOAClient *minio.MinioClient,
	kafkaProducer *kafka.Producer,
) (*asr.ASRClient, *call.CallService, *deadletter.DeadLetterService, *deadletter.DeadLetterWorker, error) {
	logging.Logger.Info("[NewApp] Creating ASR client...")

	asrClient := asr.NewClient()

	logging.Logger.Info("[NewApp] ASR client created")

	logging.Logger.Info("[NewApp] Creating call service...")

	callService := call.NewService(dbConn, minioIVAClient, minioCOAClient, asrClient)

	logging.Logger.Info("[NewApp] Call service created")

	logging.Logger.Info("[NewApp] Creating dead letter service...")

	deadletterService := deadletter.NewService(dbConn, kafkaProducer, callService)

	logging.Logger.Info("[NewApp] Dead letter service created")

	logging.Logger.Info("[NewApp] Creating dead letter worker...")

	deadletterWorker, err := deadletter.NewWorker(deadletterService, dbConn)
	if err != nil {
		logging.Logger.Error("[NewApp] Failed to create dead letter worker", zap.Error(err))
		return nil, nil, nil, nil, err
	}

	logging.Logger.Info("[NewApp] Dead letter worker created")

	return asrClient, callService, deadletterService, deadletterWorker, nil
}

func (app *Grogu) Run(ctx context.Context) error {
	logging.Logger.Info("[Run] Starting app goroutines...")

	logging.Logger.Info("[Run] Starting health checker monitor goroutine")

	go app.HealthCheckerService.Monitor()

	logging.Logger.Info("[Run] Starting dead letter worker goroutine")

	go app.DeadLetterWorker.Run(ctx)

	err := app.runKafkaConsumer(ctx)
	if err != nil {
		return err
	}

	app.shutdown()

	return nil
}

func (app *Grogu) runKafkaConsumer(ctx context.Context) error {
	switch app.SourcePlatform {
	case "Telc":
		return app.runTelcConsumer(ctx)
	case "iva":
		return app.runIvaConsumer(ctx)
	default:
		return nil
	}
}

func (app *Grogu) runTelcConsumer(ctx context.Context) error {
	logging.Logger.Info("[Run] Starting Telc Kafka consumer (BLOCKING)",
		zap.String("topic", config.Conf.KafkaASRTopic),
		zap.Int("worker_pool_size", config.Conf.PoolSize),
	)

	err := app.KafkaConsumer.Consume(ctx, config.Conf.KafkaASRTopic, app.MessageHandler)
	if err != nil {
		logging.Logger.Error("[Run] Kafka consumer returned error", zap.Error(err))
		return err
	}

	logging.Logger.Warn("[Run] Kafka consumer returned (context canceled or error), beginning shutdown...")
	app.closeTelcConsumer()

	return nil
}

func (app *Grogu) runIvaConsumer(ctx context.Context) error {
	logging.Logger.Info("[Run] Starting IVA Kafka consumer (BLOCKING)",
		zap.String("topic", config.Conf.KafkaIvaTopic),
		zap.Int("worker_pool_size", config.Conf.PoolSize),
	)

	err := app.KafkaIvaConsumer.Consume(ctx, config.Conf.KafkaIvaTopic, app.IvaMessageHandler)
	if err != nil {
		logging.Logger.Error("[Run] IVA Kafka consumer returned error", zap.Error(err))
		return err
	}

	logging.Logger.Warn("[Run] IVA Kafka consumer returned (context canceled or error), beginning shutdown...")
	app.closeIvaConsumer()

	return nil
}

func (app *Grogu) closeTelcConsumer() {
	logging.Logger.Info("[Run] Closing Telc Kafka consumer...")

	err := app.KafkaConsumer.Close()
	if err != nil {
		logging.Logger.Error("[Run] Failed to close Telc consumer", zap.String("error", err.Error()))
	} else {
		logging.Logger.Info("[Run] Telc consumer closed successfully")
	}
}

func (app *Grogu) closeIvaConsumer() {
	logging.Logger.Info("[Run] Closing IVA Kafka consumer...")

	err := app.KafkaIvaConsumer.Close()
	if err != nil {
		logging.Logger.Error("[Run] Failed to close IVA consumer", zap.String("error", err.Error()))
	} else {
		logging.Logger.Info("[Run] IVA consumer closed successfully")
	}
}

func (app *Grogu) shutdown() {
	logging.Logger.Info("[Run] Releasing worker pool...",
		zap.Int("running_workers", app.WorkerPool.Running()),
		zap.Int("free_workers", app.WorkerPool.Free()),
	)
	app.WorkerPool.Release()
	logging.Logger.Info("[Run] Worker pool released")

	logging.Logger.Info("[Run] Closing Kafka producer...")

	err := app.KafkaProducer.Close()
	if err != nil {
		logging.Logger.Error("[Run] Failed to close producer", zap.String("error", err.Error()))
	} else {
		logging.Logger.Info("[Run] Kafka producer closed successfully")
	}

	logging.Logger.Info("[Run] ===== App shutdown complete =====")
}
