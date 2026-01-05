package minio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/avast/retry-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
)

const PlatformIva = "iva"

var (
	ErrConvertToStringUrl = errors.New("failed to convert result url to string")
	ErrConvertToBuffer    = errors.New("failed to convert result to pointer to bytes.Buffer")
)

type MinioClient struct {
	Client         *minio.Client
	CircuitBreaker *gobreaker.CircuitBreaker[any]
}

// NewMinioClient initializes a MinIO client with secure HTTPS connection
// Uses IVA-specific config when SourcePlatform is "iva", otherwise uses Telc config
func NewMinioClient() (*MinioClient, error) {
	var endpointURL, accessKey, secretKey string

	// Select configuration based on platform
	if config.Conf.SourcePlatform == PlatformIva {
		endpointURL = config.Conf.MinioEndpointURL
		accessKey = config.Conf.MinioIvaAccessKey
		secretKey = config.Conf.MinioIvaSecretKey
	} else {
		endpointURL = config.Conf.MinioEndpointURL
		accessKey = config.Conf.MinioAccessKey
		secretKey = config.Conf.MinioSecretKey
	}

	client, err := minio.New(endpointURL, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
	})
	if err != nil {
		logging.Logger.Error("Failed to initialize MinIO client",
			zap.String("platform", config.Conf.SourcePlatform),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	bucketName := config.Conf.MinioBucketName
	if config.Conf.SourcePlatform == PlatformIva {
		bucketName = config.Conf.MinioIvaBucketName
	}

	logging.Logger.Info("Successfully connected to MinIO",
		zap.String("platform", config.Conf.SourcePlatform),
		zap.String("endpoint", endpointURL),
		zap.String("bucket", bucketName),
	)

	return &MinioClient{
		Client:         client,
		CircuitBreaker: newCircuitBreaker(),
	}, nil
}

func newCircuitBreaker() *gobreaker.CircuitBreaker[any] {
	settings := gobreaker.Settings{
		Name:     "minio",
		Interval: time.Duration(config.Conf.MinioIntervalCB) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= config.Conf.MinioConsecutiveFailuresCB
		},
		OnStateChange: func(name string, fromState, toState gobreaker.State) {
			logging.Logger.Warn(
				"Circuit state changed",
				zap.String("service", name),
				zap.String("from", fromState.String()),
				zap.String("to", toState.String()),
			)

			if toState == gobreaker.StateOpen {
				circuitbreak.TriggerError(circuitbreak.MinioService)
			}
		},
	}

	return gobreaker.NewCircuitBreaker[any](settings)
}

// Helper functions to get platform-specific config values
func getBucketName() string {
	if config.Conf.SourcePlatform == PlatformIva {
		return config.Conf.MinioIvaBucketName
	}

	return config.Conf.MinioBucketName
}

func getPathPrefix() string {
	if config.Conf.SourcePlatform == PlatformIva {
		return config.Conf.MinioIvaPathPrefix
	}

	return config.Conf.MinioPathPrefix
}

func getEndpointURL() string {
	return config.Conf.MinioEndpointURL
}

func getTimeout() int {
	if config.Conf.SourcePlatform == PlatformIva {
		return config.Conf.MinioIvaTimeout
	}

	return config.Conf.MinioTimeout
}

func getMaxRetryAttempts() uint {
	if config.Conf.SourcePlatform == PlatformIva {
		return config.Conf.MinioIvaMaxRetryAttempts
	}

	return config.Conf.MinioMaxRetryAttempts
}

func getRetryBackoffMinSeconds() int {
	if config.Conf.SourcePlatform == PlatformIva {
		return config.Conf.MinioIvaRetryBackoffMinSeconds
	}

	return config.Conf.MinioRetryBackoffMinSeconds
}

func getRetryBackoffMaxSeconds() int {
	if config.Conf.SourcePlatform == PlatformIva {
		return config.Conf.MinioIvaRetryBackoffMaxSeconds
	}

	return config.Conf.MinioRetryBackoffMaxSeconds
}

// Upload uploads a buffer to MinIO with retry and returns the URL
func (m *MinioClient) Upload(ctx context.Context, buffer *bytes.Buffer, objectKey string) (string, error) {
	logging.Logger.Info("Starting MinIO upload",
		zap.String("object_key", objectKey),
		zap.Int("buffer_size", buffer.Len()),
	)

	url, err := m.CircuitBreaker.Execute(func() (any, error) {
		return m.doUpload(ctx, buffer, objectKey)
	})
	if err != nil {
		return "", err
	}

	urlStr, ok := url.(string)
	if !ok {
		return "", ErrConvertToStringUrl
	}

	return urlStr, nil
}

// Download downloads an object from MinIO with retry and returns it as a bytes.Buffer
func (m *MinioClient) Download(ctx context.Context, objectKey string) (*bytes.Buffer, error) {
	logging.Logger.Info("Starting MinIO download",
		zap.String("object_key", objectKey),
	)

	result, err := m.CircuitBreaker.Execute(func() (any, error) {
		return m.doDownload(ctx, objectKey)
	})
	if err != nil {
		return nil, err
	}

	buf, ok := result.(*bytes.Buffer)
	if !ok {
		return nil, ErrConvertToBuffer
	}

	return buf, nil
}

func (m *MinioClient) doUpload(ctx context.Context, buffer *bytes.Buffer, objectKey string) (string, error) {
	var url string

	ctxWithTimout, cancel := context.WithTimeout(ctx, time.Duration(getTimeout())*time.Second)
	defer cancel()

	err := retry.Do(
		func() error {
			_, err := m.Client.PutObject(
				ctxWithTimout,
				getBucketName(),
				m.getKey(objectKey),
				bytes.NewReader(buffer.Bytes()),
				int64(buffer.Len()),
				minio.PutObjectOptions{},
			)
			if err != nil {
				logging.Logger.Error("MinIO upload failed",
					zap.String("object_key", objectKey),
					zap.String("error", err.Error()),
				)

				return err
			}

			url = m.generateURL(objectKey)
			logging.Logger.Info("MinIO upload completed successfully",
				zap.String("object_key", objectKey),
				zap.String("url", url),
			)

			return nil
		},
		retry.Attempts(getMaxRetryAttempts()),
		retry.DelayType(retry.BackOffDelay),
		retry.Delay(time.Duration(getRetryBackoffMinSeconds())*time.Second),
		retry.MaxDelay(time.Duration(getRetryBackoffMaxSeconds())*time.Second),
	)
	if err != nil {
		logging.Logger.Error("MinIO upload failed after all retry attempts",
			zap.String("object_key", objectKey),
			zap.String("error", err.Error()),
		)

		return "", err
	}

	return url, nil
}

func (m *MinioClient) doDownload(ctx context.Context, objectKey string) (*bytes.Buffer, error) {
	var buf *bytes.Buffer

	ctxWithTimout, cancel := context.WithTimeout(ctx, time.Duration(getTimeout())*time.Second)
	defer cancel()

	err := retry.Do(
		func() error {
			object, err := m.Client.GetObject(
				ctxWithTimout,
				getBucketName(),
				m.getKey(objectKey),
				minio.GetObjectOptions{},
			)
			if err != nil {
				logging.Logger.Error("MinIO download failed",
					zap.String("object_key", objectKey),
					zap.String("error", err.Error()),
				)

				return err
			}

			defer func() {
				cerr := object.Close()
				if cerr != nil {
					logging.Logger.Error("Failed to close MinIO object reader",
						zap.String("error", cerr.Error()),
						zap.String("object", objectKey),
					)
				}
			}()

			data, err := io.ReadAll(object)
			if err != nil {
				return err
			}

			buf = bytes.NewBuffer(data)
			logging.Logger.Info("MinIO download completed successfully",
				zap.String("object_key", objectKey),
				zap.Int("size", buf.Len()),
			)

			return nil
		},
		retry.Attempts(getMaxRetryAttempts()),
		retry.DelayType(retry.BackOffDelay),
		retry.Delay(time.Duration(getRetryBackoffMinSeconds())*time.Second),
		retry.MaxDelay(time.Duration(getRetryBackoffMaxSeconds())*time.Second),
	)
	if err != nil {
		logging.Logger.Error("MinIO download failed after all retry attempts",
			zap.String("object_key", objectKey),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	return buf, nil
}

func (m *MinioClient) generateURL(objectKey string) string {
	return fmt.Sprintf("%s/%s/%s", getEndpointURL(), getBucketName(), objectKey)
}

func (m *MinioClient) getKey(objectKey string) string {
	return filepath.Join(getPathPrefix(), objectKey)
}
