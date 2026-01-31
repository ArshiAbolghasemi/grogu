package voice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"go.uber.org/zap"
)

// Static errors for duration service operations
var (
	ErrAudioBufferNilOrEmpty = errors.New("audio buffer is nil or empty")
	ErrDurationNotFound      = errors.New("duration not found in ffprobe output")
)

// DurationService handles audio duration extraction
type DurationService struct {
	TempDir string
}

// NewDurationService creates a new duration service
func NewDurationService() *DurationService {
	return &DurationService{
		TempDir: os.TempDir(),
	}
}

// ffprobeOutput represents the structure of ffprobe's JSON output
type ffprobeOutput struct {
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

// GetDuration extracts the duration (in seconds) from an audio buffer using ffprobe
func (s *DurationService) GetDuration(
	ctx context.Context,
	audioBuffer *bytes.Buffer,
	callID string,
) (int, error) {
	if audioBuffer == nil || audioBuffer.Len() == 0 {
		return 0, ErrAudioBufferNilOrEmpty
	}

	logging.Logger.Info("Extracting duration from audio file",
		zap.String("call_id", callID),
		zap.Int("buffer_size", audioBuffer.Len()),
	)

	// Create a temporary file for the audio
	tempFile, err := os.CreateTemp(s.TempDir, fmt.Sprintf("duration_%s_*.ogg", callID))
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}

	tempFilePath := tempFile.Name()

	defer func() {
		_ = os.Remove(tempFilePath)
	}()

	// Write buffer to temp file
	_, err = io.Copy(tempFile, audioBuffer)
	if err != nil {
		_ = tempFile.Close()
		return 0, fmt.Errorf("failed to write audio to temp file: %w", err)
	}

	err = tempFile.Close()
	if err != nil {
		return 0, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Use ffprobe to get duration - it returns JSON string directly
	probeData, err := ffmpeg.Probe(tempFilePath)
	if err != nil {
		logging.Logger.Error("ffprobe failed to extract duration",
			zap.String("call_id", callID),
			zap.String("error", err.Error()),
		)

		return 0, fmt.Errorf("ffprobe failed: %w", err)
	}

	// Parse ffprobe JSON output
	var output ffprobeOutput

	err = json.Unmarshal([]byte(probeData), &output)
	if err != nil {
		return 0, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	if output.Format.Duration == "" {
		return 0, ErrDurationNotFound
	}

	// Parse duration string to float64, then convert to int (seconds)
	durationFloat, err := strconv.ParseFloat(output.Format.Duration, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse duration: %w", err)
	}

	durationSeconds := int(durationFloat)

	logging.Logger.Info("Duration extracted successfully",
		zap.String("call_id", callID),
		zap.Int("duration_seconds", durationSeconds),
		zap.Float64("duration_exact", durationFloat),
	)

	return durationSeconds, nil
}
