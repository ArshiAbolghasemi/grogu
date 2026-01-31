package test

import (
	"context"
	"testing"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/call"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/deadletter"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/grogu"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/kafka"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/minio"
	"github.com/goccy/go-json"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestTelcWorkflowSuccess(t *testing.T) {
	tc := setupTelcWorkflow(t, func(mock *telcMock) {})
	defer tc.cleanup()

	sendInputMessage(t, tc.kafkaProducer, "call-success")
	assertOutputMessage(t, tc.output, "call-success")
}

func TestTelcWorkflowDeadLetter(t *testing.T) {
	tc := setupTelcWorkflow(t, func(mock *telcMock) {
		mock.failInfo.Store(true)
	})
	defer tc.cleanup()

	sendInputMessage(t, tc.kafkaProducer, "call-failure")
	assertDeadLetterRecorded(t, tc.db, "call-failure")
}

func TestTelcWorkflowCircuitBreak(t *testing.T) {
	tc := setupTelcWorkflow(t, func(mock *telcMock) {
		mock.failInfo.Store(true)
	})
	defer tc.cleanup()

	sendInputMessage(t, tc.kafkaProducer, "call-circuit")
	assertCircuitBreakTriggered(t, tc.resources.breakEvents, 5*time.Second)
}

type telcWorkflowTestContext struct {
	t             *testing.T
	resources     *dockertestResources
	appCancel     context.CancelFunc
	kafkaProducer *kafka.Producer
	output        *kafkaReader
	db            *gorm.DB
}

func (tc *telcWorkflowTestContext) cleanup() {
	tc.appCancel()
	_ = tc.output.Close()
	tc.resources.cleanup(tc.t)
}

func setupTelcWorkflow(t *testing.T, configureMock func(*telcMock)) *telcWorkflowTestContext {
	t.Helper()

	resources := newResources(t)

	circuitbreak.Init()

	telcStub := &telcMock{}
	configureMock(telcStub)
	resources.startTelcServer(t, telcStub)

	dsn := resources.startPostgres(t)
	kafkaAddr := resources.startKafka(t)
	minioEndpoint := resources.startMinio(t)

	configureConfigForTest(t, dsn, kafkaAddr, minioEndpoint, resources.telcServer.URL)

	db, err := database.NewDatabase()
	require.NoError(t, err)

	dbSchema{}.apply(t, db)

	createCallRecord(t, db, "call-success")
	createCallRecord(t, db, "call-failure")
	createCallRecord(t, db, "call-circuit")

	minioIVAClient, err := minio.NewMinioClient(
		config.Conf.MinioIvaAccessKey,
		config.Conf.MinioIvaSecretKey,
		config.Conf.MinioIvaBucketName,
		config.Conf.MinioIvaPathPrefix,
		circuitbreak.MinioIVAService,
	)
	require.NoError(t, err)

	minioCOAClient, err := minio.NewMinioClient(
		config.Conf.MinioAccessKey,
		config.Conf.MinioSecretKey,
		config.Conf.MinioBucketName,
		config.Conf.MinioPathPrefix,
		circuitbreak.MinioCOAService,
	)
	require.NoError(t, err)

	asrProvider := &fakeASR{}

	callService := call.NewService(db, minioIVAClient, minioCOAClient, asrProvider)

	kafkaProducer, err := kafka.NewProducer()
	require.NoError(t, err)

	kafkaConsumer, err := kafka.NewConsumer()
	require.NoError(t, err)

	workerPool, err := ants.NewPool(4, ants.WithPreAlloc(true))
	require.NoError(t, err)

	dlService := deadletter.NewService(db, kafkaProducer, callService)

	app := &grogu.Grogu{
		KafkaConsumer:     kafkaConsumer,
		KafkaProducer:     kafkaProducer,
		WorkerPool:        workerPool,
		CallService:       callService,
		DeadLetterService: dlService,
	}

	ctx, cancel := context.WithCancel(context.Background())

	output := newKafkaReader(t, kafkaAddr, testKafkaTopicOutbound)

	go func() {
		err := app.KafkaConsumer.Consume(ctx, testKafkaTopicInbound, app.MessageHandler)
		require.NoError(t, err)
	}()

	time.Sleep(2 * time.Second)

	return &telcWorkflowTestContext{
		t:             t,
		resources:     resources,
		appCancel:     cancel,
		kafkaProducer: kafkaProducer,
		output:        output,
		db:            db,
	}
}

func sendInputMessage(t *testing.T, producer *kafka.Producer, callID string) {
	message := call.CallMessage{
		CallID:    callID,
		CallTo:    "expert-1",
		Duration:  120,
		Online:    true,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(message)
	require.NoError(t, err)

	_, _, err = producer.SendMessage(testKafkaTopicInbound, []byte(callID), payload)
	require.NoError(t, err)
}

func assertOutputMessage(t *testing.T, reader *kafkaReader, expectedCallID string) {
	select {
	case msg := <-reader.messages:
		require.NotNil(t, msg)
		require.Equal(t, []byte(expectedCallID), msg.Key)

		var result map[string]any
		require.NoError(t, json.Unmarshal(msg.Value, &result))

		callInfo, ok := result["call_info"].(map[string]any)
		require.True(t, ok, "call_info missing in result")
		require.Equal(t, expectedCallID, callInfo["call_id"])
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for output message for %s", expectedCallID)
	}
}

func assertDeadLetterRecorded(t *testing.T, db *gorm.DB, callID string) {
	type dl struct {
		CallID string
	}

	var record dl
	require.NoError(t, db.Table("call_asr_dl").Where("call_id = ?", callID).First(&record).Error)
	require.Equal(t, callID, record.CallID)
}

func assertCircuitBreakTriggered(t *testing.T, events <-chan string, timeout time.Duration) {
	select {
	case event := <-events:
		require.Equal(t, circuitbreak.TelCService, event)
	case <-time.After(timeout):
		t.Fatalf("expected circuit break event within %s", timeout)
	}
}

func createCallRecord(t *testing.T, db *gorm.DB, callID string) {
	payload := map[string]any{
		"call_id":     callID,
		"access_code": "ac",
		"city_code":   "21",
		"service_no":  "srv",
		"call_to":     "expert-1",
		"duration":    120,
		"created_at":  time.Now(),
	}

	require.NoError(t, db.Table("call_records").Create(payload).Error)
	require.NoError(t, db.Table("calls").Create(map[string]any{
		"call_id":    callID,
		"status":     call.StatusQueued,
		"created_at": time.Now(),
	}).Error)
}
