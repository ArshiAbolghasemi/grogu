package test

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/asr"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"github.com/IBM/sarama"
	"github.com/goccy/go-json"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	testKafkaTopicInbound  = "grogu-asr-input"
	testKafkaTopicOutbound = "grogu-call-output"
	testKafkaGroup         = "grogu-asr-group"
	testBucket             = "grogu-test-bucket"
)

type fakeASR struct{}

func (f *fakeASR) GetVoiceTranscriptions(
	ctx context.Context,
	buffer *bytes.Buffer,
	callID string,
) (*asr.ASRResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return &asr.ASRResponse{
		Segments: []asr.Segment{
			{
				SegmentNumber: 1,
				Start:         0.0,
				End:           1.5,
				Text:          "hello from grogu",
				Channel:       "caller",
			},
		},
		Metadata: json.RawMessage(`{"provider":"fake-asr"}`),
	}, nil
}

type telcMock struct {
	failInfo  atomic.Bool
	failVoice atomic.Bool
}

func (m *telcMock) handler(t *testing.T) http.Handler {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		if m.failInfo.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"error"}`))

			return
		}

		callID := extractCallID(t, r)
		info := map[string]any{
			"callId":    callID,
			"startTime": "2024-01-01 12:00:00.000",
			"endTime":   "2024-01-01 12:05:00.000",
			"reasons":   []string{"reason-a", "reason-b"},
		}

		infoBytes, err := json.Marshal(info)
		require.NoError(t, err)

		body, err := json.Marshal(map[string]any{
			"message":   string(infoBytes),
			"errorCode": "",
		})
		require.NoError(t, err)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	mux.HandleFunc("/file", func(w http.ResponseWriter, r *http.Request) {
		if m.failVoice.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		callID := extractCallID(t, r)
		payload := createZipPayload(t, callID)

		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(payload)
	})

	return mux
}

func extractCallID(t *testing.T, r *http.Request) string {
	t.Helper()

	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	defer r.Body.Close()

	var req map[string]any
	require.NoError(t, json.Unmarshal(body, &req))

	if ids, ok := req["callids"].([]any); ok && len(ids) > 0 {
		if callID, ok := ids[0].(string); ok {
			return callID
		}
	}

	if callID, ok := req["call_id"].(string); ok {
		return callID
	}

	t.Fatalf("call_id not found in request: %s", string(body))

	return ""
}

func createZipPayload(t *testing.T, callID string) []byte {
	t.Helper()

	buf := new(bytes.Buffer)

	zipWriter := zip.NewWriter(buf)

	file, err := zipWriter.Create(filepath.Join("voice", callID+".wav"))
	require.NoError(t, err)

	_, err = file.Write([]byte("audio-content"))
	require.NoError(t, err)

	require.NoError(t, zipWriter.Close())

	return buf.Bytes()
}

type kafkaReader struct {
	consumer sarama.Consumer
	messages chan *sarama.ConsumerMessage
}

func newKafkaReader(t *testing.T, brokers string, topic string) *kafkaReader {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Return.Errors = true

	reader, err := sarama.NewConsumer(strings.Split(brokers, ","), cfg)
	require.NoError(t, err)

	partitionConsumer, err := reader.ConsumePartition(topic, 0, sarama.OffsetOldest)
	require.NoError(t, err)

	msgCh := make(chan *sarama.ConsumerMessage, 10)

	go func() {
		for msg := range partitionConsumer.Messages() {
			msgCh <- msg
		}
	}()

	return &kafkaReader{
		consumer: reader,
		messages: msgCh,
	}
}

func (r *kafkaReader) Close() error {
	return r.consumer.Close()
}

type dbSchema struct{}

func (dbSchema) apply(t *testing.T, db *gorm.DB) {
	t.Helper()

	createStatements := []string{
		`CREATE TABLE IF NOT EXISTS call_records (
			call_id VARCHAR(255) PRIMARY KEY,
			access_code TEXT,
			city_code TEXT,
			service_no TEXT,
			call_start_date TIMESTAMP NULL,
			call_end_date TIMESTAMP NULL,
			call_to TEXT,
			duration INT,
			reasons JSONB,
			created_at TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS calls (
			call_id VARCHAR(255) PRIMARY KEY,
			status VARCHAR(20),
			created_at TIMESTAMP DEFAULT NOW()
		);`,
		`CREATE TABLE IF NOT EXISTS experts (
			id SERIAL PRIMARY KEY,
			first_name TEXT NULL,
			last_name TEXT NULL,
			user_code VARCHAR(255) UNIQUE NOT NULL,
			phone_number TIMESTAMP NULL,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP NULL
		);`,
		`CREATE TABLE IF NOT EXISTS call_asr_dl (
			call_id VARCHAR(255) PRIMARY KEY,
			msg JSONB NOT NULL,
			error TEXT NOT NULL,
			status VARCHAR(20) DEFAULT 'pending' NOT NULL,
			retry_count INT DEFAULT 0 NOT NULL,
			last_retry_at TIMESTAMP NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);`,
	}

	for _, stmt := range createStatements {
		require.NoError(t, db.Exec(stmt).Error)
	}
}

type dockertestResources struct {
	pool           *dockertest.Pool
	postgres       *dockertest.Resource
	kafka          *dockertest.Resource
	minio          *dockertest.Resource
	telcServer     *httptest.Server
	breakEvents    chan string
	mu             sync.Mutex
	activeResource []*dockertest.Resource
}

func newResources(t *testing.T) *dockertestResources {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 3 * time.Minute

	return &dockertestResources{
		pool:        pool,
		breakEvents: make(chan string, 5),
	}
}

func (r *dockertestResources) startTelcServer(t *testing.T, mock *telcMock) {
	t.Helper()

	r.telcServer = httptest.NewServer(mock.handler(t))

	go func() {
		for service := range circuitbreak.CircuitBreakChan {
			r.breakEvents <- service
		}
	}()
}

func (r *dockertestResources) startPostgres(t *testing.T) string {
	t.Helper()

	resource, err := r.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=grogu",
			"POSTGRES_DB=grogu",
		},
		ExposedPorts: []string{"5432/tcp"},
	})
	require.NoError(t, err)

	r.track(resource)

	hostPort := resource.GetHostPort("5432/tcp")
	host := "localhost"

	port := hostPort
	if strings.Contains(hostPort, ":") {
		parsedHost, parsedPort, err := net.SplitHostPort(hostPort)
		if err == nil {
			if parsedHost != "" && parsedHost != "0.0.0.0" {
				host = parsedHost
			}

			port = parsedPort
		} else {
			parts := strings.Split(hostPort, ":")
			port = parts[len(parts)-1]
		}
	}

	dsn := fmt.Sprintf("host=%s user=grogu password=secret dbname=grogu port=%s sslmode=disable", host, port)

	require.NoError(t, r.pool.Retry(func() error {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			return err
		}

		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		defer sqlDB.Close()

		return sqlDB.Ping()
	}))

	return dsn
}

func (r *dockertestResources) startMinio(t *testing.T) string {
	t.Helper()

	resource, err := r.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "RELEASE.2024-08-17T01-24-54Z",
		Env: []string{
			"MINIO_ACCESS_KEY=minio",
			"MINIO_SECRET_KEY=minio123",
		},
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
		ExposedPorts: []string{
			"9000/tcp",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9000/tcp": {{
				HostIP:   "0.0.0.0",
				HostPort: "9000",
			}},
		},
	})
	require.NoError(t, err)

	r.track(resource)

	endpoint := resource.GetHostPort("9000/tcp")

	require.NoError(t, r.pool.Retry(func() error {
		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4("minio", "minio123", ""),
			Secure: false,
		})
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = client.MakeBucket(ctx, testBucket, minio.MakeBucketOptions{})
		if err != nil {
			errResp := minio.ToErrorResponse(err)
			if errResp.Code != "BucketAlreadyOwnedByYou" && errResp.Code != "BucketAlreadyExists" {
				return err
			}
		}

		return nil
	}))

	return endpoint
}

func (r *dockertestResources) startKafka(t *testing.T) string {
	t.Helper()

	resource, err := r.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "bitnami/kafka",
		Tag:        "3.6",
		Env: []string{
			"KAFKA_ENABLE_KRAFT=yes",
			"KAFKA_CFG_NODE_ID=1",
			"KAFKA_CFG_PROCESS_ROLES=broker,controller",
			"KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
			"KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true",
			"ALLOW_PLAINTEXT_LISTENER=yes",
			"KAFKA_KRAFT_CLUSTER_ID=grogu-cluster",
		},
		ExposedPorts: []string{
			"9092/tcp",
			"9093/tcp",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{
				HostIP:   "0.0.0.0",
				HostPort: "9092",
			}},
			"9093/tcp": {{
				HostIP:   "0.0.0.0",
				HostPort: "9093",
			}},
		},
	})
	require.NoError(t, err)

	r.track(resource)

	broker := resource.GetHostPort("9092/tcp")

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Producer.Return.Successes = true

	require.NoError(t, r.pool.Retry(func() error {
		admin, err := sarama.NewClusterAdmin([]string{broker}, cfg)
		if err != nil {
			return err
		}
		defer admin.Close()

		err = admin.CreateTopic(testKafkaTopicInbound, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil && !strings.Contains(err.Error(), "Topic with this name already exists") {
			return err
		}

		return nil
	}))

	require.NoError(t, r.pool.Retry(func() error {
		admin, err := sarama.NewClusterAdmin([]string{broker}, cfg)
		if err != nil {
			return err
		}
		defer admin.Close()

		err = admin.CreateTopic(testKafkaTopicOutbound, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil && !strings.Contains(err.Error(), "Topic with this name already exists") {
			return err
		}

		return nil
	}))

	return broker
}

func (r *dockertestResources) cleanup(t *testing.T) {
	t.Helper()

	for _, res := range r.activeResource {
		_ = r.pool.Purge(res)
	}

	if r.telcServer != nil {
		r.telcServer.Close()
	}

	close(r.breakEvents)
}

func (r *dockertestResources) track(res *dockertest.Resource) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.activeResource = append(r.activeResource, res)
}

func configureConfigForTest(t *testing.T, dsn, kafkaAddr, minioEndpoint, telcURL string) {
	t.Helper()

	host, port := parsePostgresDSN(dsn)
	if host == "" {
		host = "localhost"
	}

	if port == "" {
		port = "5432"
	}

	config.Conf.SourcePlatform = "Telc"
	config.Conf.PostgresHost = host
	config.Conf.PostgresPort = port
	config.Conf.PostgresUsername = "grogu"
	config.Conf.PostgresPassword = "secret"
	config.Conf.PostgresDatabase = "grogu"
	config.Conf.DBIntervalCB = 1
	config.Conf.DBConsecutiveFailuresCB = 3

	config.Conf.KafkaBootstrapServer = kafkaAddr
	config.Conf.KafkaUsername = "user"
	config.Conf.KafkaPassword = "pass"
	config.Conf.KafkaASRTopic = testKafkaTopicInbound
	config.Conf.KafkaCallTopic = testKafkaTopicOutbound
	config.Conf.KafkaGroupID = testKafkaGroup
	config.Conf.KafkaIvaTopic = "unused"
	config.Conf.KafkaIvaGroupID = "unused"
	config.Conf.KafkaIntervalCB = 1
	config.Conf.KafkaConsecutiveFailuresCB = 1

	config.Conf.MinioEndpointURL = minioEndpoint
	config.Conf.MinioAccessKey = "minio"
	config.Conf.MinioSecretKey = "minio123"
	config.Conf.MinioBucketName = testBucket
	config.Conf.MinioPathPrefix = "records"
	config.Conf.MinioTimeout = 30
	config.Conf.MinioMaxRetryAttempts = 1
	config.Conf.MinioRetryBackoffMinSeconds = 1
	config.Conf.MinioRetryBackoffMaxSeconds = 2
	config.Conf.MinioIntervalCB = 1
	config.Conf.MinioConsecutiveFailuresCB = 1

	config.Conf.MinioIvaAccessKey = "minio"
	config.Conf.MinioIvaSecretKey = "minio123"
	config.Conf.MinioIvaBucketName = testBucket
	config.Conf.MinioIvaPathPrefix = "records"
	config.Conf.MinioIvaTimeout = 30

	config.Conf.ASRBaseUrl = "http://fake-asr"
	config.Conf.ASRModel = "fake-model"
	config.Conf.ASRTimeout = 10
	config.Conf.ASRRetryMaxAttempts = 1
	config.Conf.ASRRetryMinBackoff = 1
	config.Conf.ASRRetryMaxBackoff = 1
	config.Conf.ASRIntervalCB = 1
	config.Conf.ASRConsecutiveFailuresCB = 1

	config.Conf.TelcBaseUrl = telcURL
	config.Conf.TelcFileURL = "/file"
	config.Conf.TelcCallInfoUrl = "/info"
	config.Conf.TelcProxy = ""
	config.Conf.TelcTimeout = 5
	config.Conf.TelcRetryMaxAttempst = 1
	config.Conf.TelcRetryBackoffMin = 1
	config.Conf.TelcRetryBackoffMax = 1
	config.Conf.TelcMaxFileSize = 1024 * 1024
	config.Conf.TelcIntervalCB = 1
	config.Conf.TelcConsecutiveFailuresCB = 1

	config.Conf.VoiceOverlapThreshold = 1.0
	config.Conf.PoolSize = 4
	config.Conf.DeadLetterPoolSize = 2
	config.Conf.DeadLetterCallLimit = 5
	config.Conf.DeadLetterCallMaxRetries = 2
	config.Conf.DeadLetterCallInterval = 1
	config.Conf.DeadLetterCallRetryDelay = 1

	config.Conf.HealthCheckerMonitorInterval = 1
	config.Conf.HealthCheckerSampleCallId = "sample"

	config.Conf.LogFilePath = filepath.Join(os.TempDir(), "grogu-test.log")
	config.Conf.LogLevel = "INFO"
}

func parsePostgresDSN(dsn string) (string, string) {
	fields := strings.Fields(dsn)
	keyValues := map[string]string{}

	for _, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 {
			keyValues[parts[0]] = parts[1]
		}
	}

	return keyValues["host"], keyValues["port"]
}
