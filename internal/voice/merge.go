package voice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/prometheus"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"go.uber.org/zap"
)

// Static errors for voice merge operations
var (
	ErrVoiceBufferNil   = errors.New("one or both voice buffers are nil")
	ErrVoiceBufferEmpty = errors.New("one or both voice buffers are empty")
)

type MergeService struct {
	// TempDir is the directory for temporary files during merging
	TempDir string
}

func NewMergeService() *MergeService {
	return &MergeService{
		TempDir: os.TempDir(),
	}
}

// MergeVoiceFiles merges two audio files into a single stereo file
// where participantVoice goes to the left channel and agentVoice goes to the right channel
func (s *MergeService) MergeVoiceFiles(
	ctx context.Context,
	participantVoice *bytes.Buffer,
	agentVoice *bytes.Buffer,
	callID string,
) (*bytes.Buffer, error) {
	logging.Logger.Info("Merging voice files into stereo",
		zap.String("call_id", callID),
		zap.Int("participant_size", participantVoice.Len()),
		zap.Int("agent_size", agentVoice.Len()),
	)

	err := s.validateBuffers(participantVoice, agentVoice)
	if err != nil {
		return nil, err
	}

	// Create temporary files for ffmpeg processing
	participantFile, agentFile, outputFile, err := s.createTempFiles(callID)
	if err != nil {
		return nil, err
	}

	defer s.cleanupTempFiles(participantFile, agentFile, outputFile)

	// Write buffers to temp files
	err = s.writeBufferToFile(participantFile, participantVoice, "participant")
	if err != nil {
		return nil, err
	}

	err = s.writeBufferToFile(agentFile, agentVoice, "agent")
	if err != nil {
		return nil, err
	}

	// Close files before ffmpeg processes them
	err = participantFile.Close()
	if err != nil {
		logging.Logger.Warn("Failed to close participant file", zap.String("error", err.Error()))
	}

	err = agentFile.Close()
	if err != nil {
		logging.Logger.Warn("Failed to close agent file", zap.String("error", err.Error()))
	}

	logging.Logger.Debug("Temporary files created for merging",
		zap.String("call_id", callID),
		zap.String("participant_file", participantFile.Name()),
		zap.String("agent_file", agentFile.Name()),
		zap.String("output_file", outputFile),
	)

	// Run ffmpeg merge
	mergedData, err := s.runFFmpegMerge(participantFile.Name(), agentFile.Name(), outputFile, callID)
	if err != nil {
		return nil, err
	}

	logging.Logger.Info("Voice files merged successfully",
		zap.String("call_id", callID),
		zap.Int("merged_size", len(mergedData)),
	)

	return bytes.NewBuffer(mergedData), nil
}

// validateBuffers checks if the voice buffers are valid
func (s *MergeService) validateBuffers(participantVoice, agentVoice *bytes.Buffer) error {
	if participantVoice == nil || agentVoice == nil {
		return ErrVoiceBufferNil
	}

	if participantVoice.Len() == 0 || agentVoice.Len() == 0 {
		return ErrVoiceBufferEmpty
	}

	return nil
}

// createTempFiles creates temporary files for participant, agent, and output
func (s *MergeService) createTempFiles(callID string) (*os.File, *os.File, string, error) {
	participantFile, err := os.CreateTemp(s.TempDir, fmt.Sprintf("participant_%s_*.ogg", callID))
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to create temp file for participant: %w", err)
	}

	agentFile, err := os.CreateTemp(s.TempDir, fmt.Sprintf("agent_%s_*.ogg", callID))
	if err != nil {
		removeErr := os.Remove(participantFile.Name())
		if removeErr != nil {
			logging.Logger.Warn("Failed to remove participant temp file", zap.String("error", removeErr.Error()))
		}

		return nil, nil, "", fmt.Errorf("failed to create temp file for agent: %w", err)
	}

	outputFile := filepath.Join(s.TempDir, fmt.Sprintf("merged_%s.mp3", callID))

	return participantFile, agentFile, outputFile, nil
}

// cleanupTempFiles removes all temporary files
func (s *MergeService) cleanupTempFiles(participantFile, agentFile *os.File, outputFile string) {
	s.cleanupTempFile(participantFile, "participant")
	s.cleanupTempFile(agentFile, "agent")

	err := os.Remove(outputFile)
	if err != nil {
		logging.Logger.Debug("Failed to remove output temp file", zap.String("error", err.Error()))
	}
}

// cleanupTempFile closes and removes a single temporary file
func (s *MergeService) cleanupTempFile(file *os.File, name string) {
	if file == nil {
		return
	}

	err := file.Close()
	if err != nil {
		logging.Logger.Debug("Failed to close "+name+" file", zap.String("error", err.Error()))
	}

	err = os.Remove(file.Name())
	if err != nil {
		logging.Logger.Debug("Failed to remove "+name+" temp file", zap.String("error", err.Error()))
	}
}

// writeBufferToFile writes a buffer to a file
func (s *MergeService) writeBufferToFile(file *os.File, buffer *bytes.Buffer, name string) error {
	_, err := io.Copy(file, buffer)
	if err != nil {
		return fmt.Errorf("failed to write %s voice to temp file: %w", name, err)
	}

	return nil
}

// runFFmpegMerge runs the ffmpeg merge operation and returns the merged data
func (s *MergeService) runFFmpegMerge(participantPath, agentPath, outputFile, callID string) ([]byte, error) {
	mergeStartTime := time.Now()

	logging.Logger.Debug("Starting ffmpeg merge operation",
		zap.String("call_id", callID),
	)

	// Use ffmpeg to merge: participant on left channel, agent on right channel
	// Both inputs are stereo, so we downmix each to mono (taking left channel)
	// Then use 'join' filter to place participant on FL (left) and agent on FR (right)
	participant := ffmpeg.Input(participantPath).Filter("pan", ffmpeg.Args{"mono|c0=FL"})
	agent := ffmpeg.Input(agentPath).Filter("pan", ffmpeg.Args{"mono|c0=FL"})

	// Capture stderr for better error messages
	var stderrBuf bytes.Buffer

	err := ffmpeg.Filter(
		[]*ffmpeg.Stream{participant, agent},
		"join",
		ffmpeg.Args{"inputs=2:channel_layout=stereo:map=0.0-FL|1.0-FR"},
	).Output(outputFile, ffmpeg.KwArgs{
		"c:a": "libmp3lame",
		"b:a": "16000",
		"ar":  "8000",
	}).OverWriteOutput().
		WithErrorOutput(&stderrBuf).
		Run()

	mergeDuration := time.Since(mergeStartTime)
	prometheus.VoiceMergeDuration.Observe(mergeDuration.Seconds())

	if err != nil {
		logging.Logger.Error("FFmpeg merge failed",
			zap.String("call_id", callID),
			zap.Duration("merge_duration", mergeDuration),
			zap.String("error", err.Error()),
			zap.String("stderr", stderrBuf.String()),
		)

		return nil, fmt.Errorf("ffmpeg merge failed: %w - stderr: %s", err, stderrBuf.String())
	}

	logging.Logger.Info("FFmpeg merge completed",
		zap.String("call_id", callID),
		zap.Duration("merge_duration", mergeDuration),
		zap.Float64("merge_duration_ms", float64(mergeDuration.Milliseconds())),
	)

	// Read the merged output file into a buffer
	// #nosec G304 -- outputFile is constructed from controlled TempDir and callID
	mergedData, err := os.ReadFile(outputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read merged output file: %w", err)
	}

	return mergedData, nil
}
