package asr

import (
	"bytes"
	"context"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/avast/retry-go"
	"github.com/goccy/go-json"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
)

type Segment struct {
	SegmentNumber int     `json:"segment_number"`
	Start         float64 `json:"start"`
	End           float64 `json:"end"`
	Text          string  `json:"text"`
	Channel       string  `json:"channel"`
}

type ASRResponse struct {
	Segments []Segment       `json:"segments"`
	Metadata json.RawMessage `json:"metadata"`
}

type ASRClient struct {
	Client         *openai.Client
	CircuitBreaker *gobreaker.CircuitBreaker[[]byte]
}

func NewClient() *ASRClient {
	opts := []option.RequestOption{
		option.WithBaseURL(config.Conf.ASRBaseUrl),
		option.WithRequestTimeout(time.Duration(config.Conf.ASRTimeout) * time.Second),
	}

	client := openai.NewClient(opts...)

	return &ASRClient{
		Client:         &client,
		CircuitBreaker: newASRCircuitBreaker(),
	}
}

func newASRCircuitBreaker() *gobreaker.CircuitBreaker[[]byte] {
	settings := gobreaker.Settings{
		Name:     "ASRClient",
		Interval: time.Duration(config.Conf.ASRIntervalCB) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= config.Conf.ASRConsecutiveFailuresCB
		},
		OnStateChange: func(name string, fromState, toState gobreaker.State) {
			logging.Logger.Info("Circuit state changed",
				zap.String("service", name),
				zap.String("from", fromState.String()),
				zap.String("to", toState.String()),
			)

			if toState == gobreaker.StateOpen {
				circuitbreak.TriggerError(circuitbreak.ASRService)
			}
		},
	}

	return gobreaker.NewCircuitBreaker[[]byte](settings)
}

// GetVoiceTranscriptions transcribes audio file to text using OpenAI API
func (asrClient *ASRClient) GetVoiceTranscriptions(
	ctx context.Context,
	buffer *bytes.Buffer,
	callID string,
) (*ASRResponse, error) {
	logging.Logger.Info("Starting voice transcription",
		zap.String("call_id", callID),
		zap.Int("buffer_size", buffer.Len()),
	)

	result, err := asrClient.CircuitBreaker.Execute(func() ([]byte, error) {
		return asrClient.doASRRequest(ctx, buffer, callID)
	})
	if err != nil {
		return nil, err
	}

	var asrRes ASRResponse

	err = json.Unmarshal(result, &asrRes)
	if err != nil {
		return nil, err
	}

	return &asrRes, nil
}

func (asrClient *ASRClient) doASRRequest(ctx context.Context, buffer *bytes.Buffer, callID string) ([]byte, error) {
	var resultBytes []byte

	err := retry.Do(
		func() error {
			reader := bytes.NewReader(buffer.Bytes())
			opts := []option.RequestOption{
				option.WithHeader("x-request-id", callID),
			}

			resp, err := asrClient.Client.Audio.Transcriptions.New(ctx, openai.AudioTranscriptionNewParams{
				File:  reader,
				Model: config.Conf.ASRModel,
			}, opts...)
			if err != nil {
				logging.Logger.Error("Transcription request failed",
					zap.String("call_id", callID),
					zap.String("error", err.Error()),
				)

				return err
			}

			resultBytes = []byte(resp.Text)
			logging.Logger.Info("Transcription completed successfully",
				zap.String("call_id", callID),
				zap.Int("text_length", len(resultBytes)),
			)

			return nil
		},
		retry.Attempts(config.Conf.ASRRetryMaxAttempts),
		retry.DelayType(retry.BackOffDelay),
		retry.Delay(time.Duration(config.Conf.ASRRetryMinBackoff)*time.Second),
		retry.MaxDelay(time.Duration(config.Conf.ASRRetryMaxBackoff)*time.Second),
	)
	if err != nil {
		logging.Logger.Error("Transcription failed after all retry attempts",
			zap.String("call_id", callID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	return resultBytes, nil
}
