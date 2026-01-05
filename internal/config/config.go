package config

import (
	"errors"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Config struct {
	SourcePlatform string `mapstructure:"source_platform" validate:"required,oneof=Telc iva"`

	ASRBaseUrl               string `mapstructure:"asr_base_url"                validate:"required"`
	ASRTimeout               int    `mapstructure:"asr_timeout"`
	ASRModel                 string `mapstructure:"asr_model"                   validate:"required"`
	ASRRetryMaxAttempts      uint   `mapstructure:"asr_retry_max_attempts"`
	ASRRetryMinBackoff       int    `mapstructure:"asr_retry_min_backoff"`
	ASRRetryMaxBackoff       int    `mapstructure:"asr_retry_max_backoff"`
	ASRIntervalCB            uint32 `mapstructure:"asr_interval_cb"`
	ASRConsecutiveFailuresCB uint32 `mapstructure:"asr_consecutive_failures_cb"`

	PostgresHost            string `mapstructure:"postgres_host"              validate:"required"`
	PostgresUsername        string `mapstructure:"postgres_username"          validate:"required"`
	PostgresPassword        string `mapstructure:"postgres_password"          validate:"required"`
	PostgresPort            string `mapstructure:"postgres_port"              validate:"required"`
	PostgresDatabase        string `mapstructure:"postgres_database"          validate:"required"`
	DBIntervalCB            uint32 `mapstructure:"db_interval_cb"`
	DBConsecutiveFailuresCB uint32 `mapstructure:"db_consecutive_failures_cb"`

	KafkaBootstrapServer       string `mapstructure:"kafka_bootstrap_server"        validate:"required"`
	KafkaUsername              string `mapstructure:"kafka_username"                validate:"required"`
	KafkaPassword              string `mapstructure:"kafka_password"                validate:"required"`
	KafkaASRTopic              string `mapstructure:"kafka_asr_call_topic"          validate:"required"`
	KafkaCallTopic             string `mapstructure:"kafka_call_topic"              validate:"required"`
	KafkaGroupID               string `mapstructure:"kafka_asr_call_group_id"       validate:"required"`
	KafkaIvaTopic              string `mapstructure:"kafka_iva_topic"               validate:"required"`
	KafkaIvaGroupID            string `mapstructure:"kafka_iva_group_id"            validate:"required"`
	KafkaIntervalCB            uint32 `mapstructure:"kafka_interval_cb"`
	KafkaConsecutiveFailuresCB uint32 `mapstructure:"kafka_consecutive_failures_cb"`

	LogLevel    string `mapstructure:"log_level"`
	LogFilePath string `mapstructure:"log_file_path"`

	MinioEndpointURL            string `mapstructure:"minio_endpoint_url"              validate:"required"`
	MinioAccessKey              string `mapstructure:"minio_access_key"                validate:"required"`
	MinioSecretKey              string `mapstructure:"minio_secret_key"                validate:"required"`
	MinioBucketName             string `mapstructure:"minio_bucket_name"               validate:"required"`
	MinioMaxRetryAttempts       uint   `mapstructure:"minio_max_retry_attempts"`
	MinioRetryBackoffMinSeconds int    `mapstructure:"minio_retry_backoff_min_seconds"`
	MinioRetryBackoffMaxSeconds int    `mapstructure:"minio_retry_backoff_max_seconds"`
	MinioPathPrefix             string `mapstructure:"minio_path_prefix"               validate:"required"`
	MinioTimeout                int    `mapstructure:"minio_timeout"`
	MinioIntervalCB             uint32 `mapstructure:"minio_interval_cb"`
	MinioConsecutiveFailuresCB  uint32 `mapstructure:"minio_consecutive_failures_cb"`

	// IVA-specific Minio configuration (only used when SOURCE_PLATFORM=iva)
	MinioIvaAccessKey              string `mapstructure:"minio_iva_access_key"                validate:"required"`
	MinioIvaSecretKey              string `mapstructure:"minio_iva_secret_key"                validate:"required"`
	MinioIvaBucketName             string `mapstructure:"minio_iva_bucket_name"               validate:"required"`
	MinioIvaMaxRetryAttempts       uint   `mapstructure:"minio_iva_max_retry_attempts"`
	MinioIvaRetryBackoffMinSeconds int    `mapstructure:"minio_iva_retry_backoff_min_seconds"`
	MinioIvaRetryBackoffMaxSeconds int    `mapstructure:"minio_iva_retry_backoff_max_seconds"`
	MinioIvaPathPrefix             string `mapstructure:"minio_iva_path_prefix"`
	MinioIvaTimeout                int    `mapstructure:"minio_iva_timeout"`

	PoolSize           int `mapstructure:"pool_size"`
	DeadLetterPoolSize int `mapstructure:"dead_letter_pool_size"`

	TelcBaseUrl               string `mapstructure:"telc_base_url"                validate:"required"`
	TelcAPIKey                string `mapstructure:"telc_api_key"                 validate:"required"`
	TelcTimeout               int    `mapstructure:"telc_timeout"                 validate:"required"`
	TelcProxy                 string `mapstructure:"telc_proxy"                   validate:"required"`
	TelcFileURL               string `mapstructure:"telc_file_url"                validate:"required"`
	TelcCallInfoUrl           string `mapstructure:"telc_call_info_url"           validate:"required"`
	TelcRetryMaxAttempst      uint   `mapstructure:"telc_retry_max_attempts"`
	TelcRetryBackoffMax       int    `mapstructure:"telc_retry_backoff_max"`
	TelcRetryBackoffMin       int    `mapstructure:"telc_retry_backoff_min"`
	TelcMaxFileSize           int64  `mapstructure:"telc_max_voice_file_size"`
	TelcIntervalCB            uint32 `mapstructure:"telc_interval_cb"`
	TelcConsecutiveFailuresCB uint32 `mapstructure:"telc_consecutive_failures_cb"`

	VoiceOverlapThreshold float64 `mapstructure:"voice_overlap_threshold"`

	DeadLetterCallMaxRetries int `mapstructure:"deadletter_call_max_retries"`
	DeadLetterCallLimit      int `mapstructure:"deadletter_call_limit"`
	DeadLetterCallInterval   int `mapstructure:"deadletter_call_interval"`
	DeadLetterCallRetryDelay int `mapstructure:"deadletter_call_retry_delay"`

	HealthCheckerMonitorInterval int    `mapstructure:"health_checker_monitor_interval"`
	HealthCheckerSampleCallId    string `mapstructure:"health_checker_sample_call_id"   validate:"required"`

	PrometheusPort    string `mapstructure:"prometheus_port"`
	PrometheusTimeout int    `mapstructure:"prometheus_timeout"`
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
		viper.SetDefault(field.Tag.Get("mapstructure"), "")
	}

	viper.SetDefault("ASR_TIMEOUT", "30")
	viper.SetDefault("ASR_RETRY_MAX_ATTEMPPTS", "3")
	viper.SetDefault("ASR_RETRY_MIN_BACKOFF", "1")
	viper.SetDefault("ASR_RETRY_MAX_BACKOFF", "10")
	viper.SetDefault("LOG_LEVEL", "INFO")
	viper.SetDefault("LOG_FILE_PATH", "./access.log")
	viper.SetDefault("POOL_SIZE", "10")
	viper.SetDefault("VOICE_OVERLAP_THRESHOLD", "1.0")
	viper.SetDefault("MINIO_MAX_RETRY_ATTEMPTS", "3")
	viper.SetDefault("MINIO_RETRY_BACKOFF_MIN_SECONDS", "1")
	viper.SetDefault("MINIO_RETRY_BACKOFF_MAX_SECONDS", "10")
	viper.SetDefault("MINIO_TIMEOUT", "60")
	viper.SetDefault("TELC_TIMEOUT", "30")
	viper.SetDefault("TELC_RETRY_MAX_ATTEMPTS", "3")
	viper.SetDefault("TELC_RETRY_BACKOFF_MIN", "1")
	viper.SetDefault("TELC_RETRY_BACKOFF_MAX", "10")
	viper.SetDefault("TELC_MAX_VOICE_FILE_SIZE", "10485760")
	viper.SetDefault("DEADLETTER_CALL_MAX_RETRIES", "10")
	viper.SetDefault("DEADLETTER_CALL_LIMIT", "100")
	viper.SetDefault("DEADLETTER_CALL_INTERVAL", "1")
	viper.SetDefault("DEADLETTER_CALL_RETRY_DELAY", "1")
	viper.SetDefault("DEAD_LETTER_POOL_SIZE", "3")
	viper.SetDefault("ASR_INTERVAL_CB", "30")
	viper.SetDefault("ASR_CONSECUTIVE_FAILURES_CB", "3")
	viper.SetDefault("TELC_INTERVAL_CB", "30")
	viper.SetDefault("TELC_CONSECUTIVE_FAILURES_CB", "3")
	viper.SetDefault("HEALTH_CHECKER_MONITOR_INTERVAL", "60")
	viper.SetDefault("DB_INTERVAL_CB", "30")
	viper.SetDefault("DB_CONSECUTIVE_FAILURES_CB", "3")
	viper.SetDefault("MINIO_INTERVAL_CB", "300")
	viper.SetDefault("MINIO_CONSECUTIVE_FAILURES_CB", "3")
	viper.SetDefault("KAFKA_INTERVAL_CB", "30")
	viper.SetDefault("KAFKA_CONSECUTIVE_FAILURES_CB", "5")
	viper.SetDefault("PROMETHEUS_PORT", "2112")
	viper.SetDefault("PROMETHEUS_TIMEOUT", "60")
}
