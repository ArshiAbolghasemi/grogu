package prometheus

import "github.com/prometheus/client_golang/prometheus"

const (
	MessageDurationBucketStart  = 4.0
	MessageDurationBucketFactor = 2.0
	MessageDurationBucketCount  = 17
)

const (
	voiceMergeBucketStart  = 0.5
	voiceMergeBucketFactor = 2
	voiceMergeBucketCount  = 8
)

const (
	kafkaLatencyBucketStart  = 1.0
	kafkaLatencyBucketFactor = 2.5
	kafkaLatencyBucketCount  = 15
)

var ProcessMessageDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "process_message_duration_seconds",
		Help: "Time taken to process a message",
		Buckets: prometheus.ExponentialBuckets(
			MessageDurationBucketStart,
			MessageDurationBucketFactor,
			MessageDurationBucketCount,
		),
	},
	[]string{"source_platform"},
)

var VoiceMergeDuration = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name: "voice_merge_duration_seconds",
		Help: "Time taken to merge two voice files",
		Buckets: prometheus.ExponentialBuckets(
			voiceMergeBucketStart,
			voiceMergeBucketFactor,
			voiceMergeBucketCount,
		),
	},
)

var KafkaMessageLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "kafka_message_latency_seconds",
		Help: "Time taken from message production to consumption",
		Buckets: prometheus.ExponentialBuckets(
			kafkaLatencyBucketStart,
			kafkaLatencyBucketFactor,
			kafkaLatencyBucketCount,
		),
	},
	[]string{"source_platform"},
)

func init() {
	prometheus.MustRegister(ProcessMessageDuration)
	prometheus.MustRegister(VoiceMergeDuration)
	prometheus.MustRegister(KafkaMessageLatency)
}
