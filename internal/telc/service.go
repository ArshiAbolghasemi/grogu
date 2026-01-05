package telc

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"github.com/avast/retry-go"
	"github.com/goccy/go-json"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
)

var (
	ErrGetFileInfoRequest = errors.New("telc get file info request failed")
	ErrGetVoiceFileReques = errors.New("telc get voice file request failed")
	ErrTelCServerError    = errors.New("telc server error")
)

type telCCallInfoResponse struct {
	Message   string `json:"message"`
	ErrorCode string `json:"errorCode"`
}

type telCCallInfoRaw struct {
	CallID    string          `json:"callId"`
	StartTime string          `json:"startTime"`
	EndTime   string          `json:"endTime"`
	Reasons   json.RawMessage `json:"reasons"`
}

type TelCCallInfo struct {
	CallID    string          `json:"callId"`
	StartTime *time.Time      `json:"startTime,omitempty"`
	EndTime   *time.Time      `json:"endTime,omitempty"`
	Reasons   json.RawMessage `json:"reasons,omitempty"`
}

type TelCService struct {
	CircuitBreaker *gobreaker.CircuitBreaker[[]byte]
}

func NewService() *TelCService {
	cbSettings := gobreaker.Settings{
		Name:     "TelC",
		Interval: time.Duration(config.Conf.TelcIntervalCB) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= config.Conf.TelcConsecutiveFailuresCB
		},
		OnStateChange: func(name string, fromSate, toSate gobreaker.State) {
			logging.Logger.Info("Circuit state changed",
				zap.String("service", name),
				zap.String("from", fromSate.String()),
				zap.String("to", toSate.String()),
			)

			if toSate == gobreaker.StateOpen {
				circuitbreak.TriggerError(circuitbreak.TelCService)
			}
		},
		IsSuccessful: func(err error) bool {
			return !errors.Is(err, ErrTelCServerError)
		},
	}

	return &TelCService{
		CircuitBreaker: gobreaker.NewCircuitBreaker[[]byte](cbSettings),
	}
}

// GetCallInfo return call info from telc api
func (telcService *TelCService) GetCallInfo(ctx context.Context, callID string) (*TelCCallInfo, error) {
	apiUrl, err := url.JoinPath(config.Conf.TelcBaseUrl, config.Conf.TelcCallInfoUrl)
	if err != nil {
		return nil, err
	}

	reqBody, err := json.Marshal(map[string]string{"call_id": callID})
	if err != nil {
		return nil, err
	}

	body, statusCode, err := telcService.doTelCRequestWithRetry(ctx, apiUrl, reqBody)
	if err != nil {
		return nil, err
	}

	logging.Logger.Info("Call Info response",
		zap.String("call_id", callID),
		zap.Int("status_code", statusCode),
		zap.ByteString("response_body", body),
	)

	if statusCode != http.StatusOK {
		return nil, ErrGetFileInfoRequest
	}

	var response telCCallInfoResponse

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	var raw telCCallInfoRaw

	err = json.Unmarshal([]byte(response.Message), &raw)
	if err != nil {
		return nil, err
	}

	start, err := parseTelCTime(raw.StartTime)
	if err != nil {
		return nil, err
	}

	end, err := parseTelCTime(raw.EndTime)
	if err != nil {
		return nil, err
	}

	return &TelCCallInfo{
		CallID:    raw.CallID,
		StartTime: start,
		EndTime:   end,
		Reasons:   raw.Reasons,
	}, nil
}

var (
	telcLayout = "2006-01-02 15:04:05.000"
	tehranLoc  *time.Location
)

func init() {
	var err error

	tehranLoc, err = time.LoadLocation("Asia/Tehran")
	if err != nil {
		panic(fmt.Sprintf("failed to load Asia/Tehran location: %v", err))
	}
}

func parseTelCTime(s string) (*time.Time, error) {
	if s == "" {
		return &time.Time{}, nil
	}

	t, err := time.ParseInLocation(telcLayout, s, tehranLoc)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// GetVoiceCall return voice call using telc apo
func (telcService *TelCService) GetVoiceCall(ctx context.Context, callID string) (*bytes.Buffer, error) {
	apiUrl, err := url.JoinPath(config.Conf.TelcBaseUrl, config.Conf.TelcFileURL)
	if err != nil {
		return nil, err
	}

	reqBody, err := json.Marshal(map[string][]string{"callids": {callID}})
	if err != nil {
		return nil, err
	}

	content, statusCode, err := telcService.doTelCRequestWithRetry(ctx, apiUrl, reqBody)
	if err != nil {
		return nil, err
	}

	logging.Logger.Info("Voice call response document",
		zap.String("call_id", callID),
		zap.Int("status_code", statusCode),
		zap.Int("content_length", len(content)),
	)

	if statusCode != http.StatusOK {
		return nil, ErrGetVoiceFileReques
	}

	return telcService.unzipVoiceFile(content, callID)
}

func (telcService *TelCService) doTelCRequestWithRetry(
	ctx context.Context,
	apiUrl string,
	reqBody []byte,
) ([]byte, int, error) {
	var (
		body       []byte
		statusCode int
	)

	body, err := telcService.CircuitBreaker.Execute(func() ([]byte, error) {
		err := retry.Do(
			func() error {
				var err error

				body, statusCode, err = telcService.doTelCRequest(ctx, apiUrl, reqBody)

				return err
			},
			retry.Attempts(config.Conf.TelcRetryMaxAttempst),
			retry.DelayType(retry.BackOffDelay),
			retry.Delay(time.Duration(config.Conf.TelcRetryBackoffMin)*time.Second),
			retry.MaxDelay(time.Duration(config.Conf.TelcRetryBackoffMax)*time.Second),
		)

		if statusCode < http.StatusOK || statusCode >= http.StatusInternalServerError {
			return nil, ErrTelCServerError
		}

		if err != nil {
			return nil, err
		}

		return body, nil
	})
	if err != nil {
		return nil, 0, err
	}

	return body, statusCode, nil
}

func (telcService *TelCService) doTelCRequest(ctx context.Context, apiUrl string, reqBody []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiUrl, bytes.NewReader(reqBody))
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Authorization", "Basic "+config.Conf.TelcAPIKey)
	req.Header.Set("Content-Type", "application/json;charset=utf-8")

	proxyUrl, err := url.Parse(config.Conf.TelcProxy)
	if err != nil {
		return nil, 0, err
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyUrl),
	}

	client := &http.Client{
		Timeout:   time.Duration(config.Conf.TelcTimeout) * time.Second,
		Transport: transport,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil {
			logging.Logger.Error("Failed to close response body", zap.String("error", cerr.Error()))
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	return body, resp.StatusCode, nil
}

var (
	ErrEmptyZip              = errors.New("zip file is empty")
	ErrCallFileNotFoundInZip = errors.New("call file does not exist in zip")
	ErrInvalidZip            = errors.New("invalid ZIP file returned by TelC API")
)

func (telcService *TelCService) unzipVoiceFile(content []byte, callID string) (*bytes.Buffer, error) {
	reader, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidZip, err)
	}

	if len(reader.File) == 0 {
		return nil, ErrEmptyZip
	}

	var file *zip.File
	for _, f := range reader.File {
		baseName := path.Base(f.Name)
		nameWithoutExt := baseName[:len(baseName)-len(path.Ext(baseName))]

		if nameWithoutExt == callID {
			file = f
			break
		}
	}

	if file == nil {
		return nil, ErrCallFileNotFoundInZip
	}

	fileReader, err := file.Open()
	if err != nil {
		return nil, err
	}

	defer func() {
		err := fileReader.Close()
		if err != nil {
			logging.Logger.Error("Failed to close zip file reader", zap.String("error", err.Error()))
		}
	}()

	buf := new(bytes.Buffer)

	_, err = io.Copy(buf, io.LimitReader(fileReader, config.Conf.TelcMaxFileSize))
	if err != nil {
		return nil, err
	}

	return buf, nil
}
