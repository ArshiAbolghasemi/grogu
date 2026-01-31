package config

import (
	"errors"
	"os"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const maxParentLevel = 6

type Config struct {
	SourcePlatform string `mapstructure:"source_platform" maptest:"Telc" validate:"required,oneof=Telc iva"`

	ASRBaseUrl string `mapstructure:"asr_base_url" maptest:"http://fake-asr"  validate:"required"`
	ASRTimeout int    `mapdefault:"30"             mapstructure:"asr_timeout"`
	ASRModel   string `mapstructure:"asr_model"    maptest:"fake-model"       validate:"required"`

	ASRRetryMaxAttempts      uint   `mapdefault:"3"     mapstructure:"asr_retry_max_attempts"`
	ASRRetryMinBackoff       int    `mapdefault:"1"     mapstructure:"asr_retry_min_backoff"`
	ASRRetryMaxBackoff       int    `mapdefault:"10"    mapstructure:"asr_retry_max_backoff"`
	ASRIntervalCB            uint32 `mapdefault:"30"    mapstructure:"asr_interval_cb"`
	ASRConsecutiveFailuresCB uint32 `mapdefault:"3"     mapstructure:"asr_consecutive_failures_cb"`
	ASREmotion               bool   `mapdefault:"false" mapstructure:"asr_emotion"`

	PostgresHost            string `mapstructure:"postgres_host"     maptest:"localhost"                       validate:"required"`
	PostgresUsername        string `mapstructure:"postgres_username" maptest:"grogu"                           validate:"required"`
	PostgresPassword        string `mapstructure:"postgres_password" maptest:"secret"                          validate:"required"`
	PostgresPort            string `mapstructure:"postgres_port"     maptest:"5432"                            validate:"required"`
	PostgresDatabase        string `mapstructure:"postgres_database" maptest:"grogu"                           validate:"required"`
	DBIntervalCB            uint32 `mapdefault:"30"                  mapstructure:"db_interval_cb"`
	DBConsecutiveFailuresCB uint32 `mapdefault:"3"                   mapstructure:"db_consecutive_failures_cb"`

	KafkaBootstrapServer       string `mapstructure:"kafka_bootstrap_server"  maptest:"localhost:9092"                     validate:"required"`
	KafkaUsername              string `mapstructure:"kafka_username"          maptest:"user"                               validate:"required"`
	KafkaPassword              string `mapstructure:"kafka_password"          maptest:"pass"                               validate:"required"`
	KafkaASRTopic              string `mapstructure:"kafka_asr_call_topic"    maptest:"asr-calls"                          validate:"required"`
	KafkaCallTopic             string `mapstructure:"kafka_call_topic"        maptest:"calls"                              validate:"required"`
	KafkaGroupID               string `mapstructure:"kafka_asr_call_group_id" maptest:"asr-group"                          validate:"required"`
	KafkaIvaTopic              string `mapstructure:"kafka_iva_topic"         maptest:"iva-topic"                          validate:"required"`
	KafkaIvaGroupID            string `mapstructure:"kafka_iva_group_id"      maptest:"iva-group"                          validate:"required"`
	KafkaIntervalCB            uint32 `mapdefault:"30"                        mapstructure:"kafka_interval_cb"`
	KafkaConsecutiveFailuresCB uint32 `mapdefault:"5"                         mapstructure:"kafka_consecutive_failures_cb"`

	LogLevel    string `mapdefault:"INFO"         mapstructure:"log_level"`
	LogFilePath string `mapdefault:"./access.log" mapstructure:"log_file_path"`

	MinioEndpointURL            string `mapstructure:"minio_endpoint_url" maptest:"localhost:9000"                       validate:"required"`
	MinioAccessKey              string `mapstructure:"minio_access_key"   maptest:"minio"                                validate:"required"`
	MinioSecretKey              string `mapstructure:"minio_secret_key"   maptest:"minio123"                             validate:"required"`
	MinioBucketName             string `mapstructure:"minio_bucket_name"  maptest:"grogu-test"                           validate:"required"`
	MinioMaxRetryAttempts       uint   `mapdefault:"3"                    mapstructure:"minio_max_retry_attempts"`
	MinioRetryBackoffMinSeconds int    `mapdefault:"1"                    mapstructure:"minio_retry_backoff_min_seconds"`
	MinioRetryBackoffMaxSeconds int    `mapdefault:"10"                   mapstructure:"minio_retry_backoff_max_seconds"`
	MinioPathPrefix             string `mapstructure:"minio_path_prefix"  maptest:"records"                              validate:"required"`
	MinioTimeout                int    `mapdefault:"60"                   mapstructure:"minio_timeout"`
	MinioIntervalCB             uint32 `mapdefault:"300"                  mapstructure:"minio_interval_cb"`
	MinioConsecutiveFailuresCB  uint32 `mapdefault:"3"                    mapstructure:"minio_consecutive_failures_cb"`

	// IVA-specific Minio configuration
	MinioIvaAccessKey              string `mapstructure:"minio_iva_access_key"                maptest:"minio"      validate:"required"`
	MinioIvaSecretKey              string `mapstructure:"minio_iva_secret_key"                maptest:"minio123"   validate:"required"`
	MinioIvaBucketName             string `mapstructure:"minio_iva_bucket_name"               maptest:"grogu-test" validate:"required"`
	MinioIvaMaxRetryAttempts       uint   `mapstructure:"minio_iva_max_retry_attempts"`
	MinioIvaRetryBackoffMinSeconds int    `mapstructure:"minio_iva_retry_backoff_min_seconds"`
	MinioIvaRetryBackoffMaxSeconds int    `mapstructure:"minio_iva_retry_backoff_max_seconds"`
	MinioIvaPathPrefix             string `mapstructure:"minio_iva_path_prefix"`
	MinioIvaTimeout                int    `mapstructure:"minio_iva_timeout"`

	PoolSize           int `mapdefault:"10" mapstructure:"pool_size"`
	DeadLetterPoolSize int `mapdefault:"3"  mapstructure:"dead_letter_pool_size"`

	TelcBaseUrl     string `mapstructure:"telc_base_url"      maptest:"http://localhost"       validate:"required"`
	TelcAPIKey      string `mapstructure:"telc_api_key"       maptest:"test-key"               validate:"required"`
	TelcTimeout     int    `mapdefault:"30"                   mapstructure:"telc_timeout"      validate:"required"`
	TelcProxy       string `mapstructure:"telc_proxy"         maptest:"http://localhost/proxy" validate:"required"`
	TelcFileURL     string `mapstructure:"telc_file_url"      maptest:"/file"                  validate:"required"`
	TelcCallInfoUrl string `mapstructure:"telc_call_info_url" maptest:"/info"                  validate:"required"`

	TelcRetryMaxAttempst uint  `mapdefault:"3"        mapstructure:"telc_retry_max_attempts"`
	TelcRetryBackoffMax  int   `mapdefault:"10"       mapstructure:"telc_retry_backoff_max"`
	TelcRetryBackoffMin  int   `mapdefault:"1"        mapstructure:"telc_retry_backoff_min"`
	TelcMaxFileSize      int64 `mapdefault:"10485760" mapstructure:"telc_max_voice_file_size"`

	TelcIntervalCB            uint32 `mapdefault:"30"    mapstructure:"telc_interval_cb"`
	TelcConsecutiveFailuresCB uint32 `mapdefault:"3"     mapstructure:"telc_consecutive_failures_cb"`
	RecoverTelcData           bool   `mapdefault:"false" mapstructure:"recover_telc_data"`

	VoiceOverlapThreshold float64 `mapdefault:"1.0" mapstructure:"voice_overlap_threshold"`

	DeadLetterCallMaxRetries int `mapdefault:"10"  mapstructure:"deadletter_call_max_retries"`
	DeadLetterCallLimit      int `mapdefault:"100" mapstructure:"deadletter_call_limit"`
	DeadLetterCallInterval   int `mapdefault:"1"   mapstructure:"deadletter_call_interval"`
	DeadLetterCallRetryDelay int `mapdefault:"1"   mapstructure:"deadletter_call_retry_delay"`

	HealthCheckerMonitorInterval int    `mapdefault:"60"                              mapstructure:"health_checker_monitor_interval"`
	HealthCheckerSampleCallId    string `mapstructure:"health_checker_sample_call_id" maptest:"sample"                               validate:"required"`

	PrometheusPort    string `mapdefault:"2112" mapstructure:"prometheus_port"`
	PrometheusTimeout int    `mapdefault:"60"   mapstructure:"prometheus_timeout"`
}

var Conf Config

func init() {
	err := loadEnvConfig(&Conf)
	if err != nil {
		zap.NewExample().Fatal("failed to load config", zap.String("error", err.Error()))
	}
}

func loadEnvConfig(cfg *Config) error {
	viper.AutomaticEnv()
	viper.AllowEmptyEnv(true)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if strings.Contains(strings.Join(os.Args, " "), "test") {
		setupTestEnv()
	}

	setupDefaults()

	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError

		ok := errors.As(err, &configFileNotFoundError)
		if !ok {
			return err
		}
	}

	err = viper.Unmarshal(cfg)
	if err != nil {
		return err
	}

	err = validator.New().Struct(cfg)
	if err != nil {
		return err
	}

	return nil
}

func setupDefaults() {
	confType := reflect.TypeOf(Conf)
	for i := range confType.NumField() {
		field := confType.Field(i)
		viper.SetDefault(field.Tag.Get("mapstructure"), field.Tag.Get("mapdefault"))
	}
}

func setupTestEnv() {
	foundPath := searchConfigPath(".", 0)
	viper.AddConfigPath(foundPath)

	confType := reflect.TypeOf(Conf)
	for i := range confType.NumField() {
		field := confType.Field(i)

		testVal := field.Tag.Get("maptest")
		if testVal == "" {
			continue
		}

		viper.Set(field.Tag.Get("mapstructure"), testVal)
	}
}

func searchConfigPath(dir string, parentLevel int8) string {
	if parentLevel > maxParentLevel {
		zap.NewExample().Error("could not find go.mod location")
		return "."
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		zap.NewExample().Fatal("failed to read directory", zap.String("error", err.Error()))
	}

	for _, file := range files {
		if file.Name() == "go.mod" {
			return dir
		}
	}

	return searchConfigPath(dir+"/..", parentLevel+1)
}
