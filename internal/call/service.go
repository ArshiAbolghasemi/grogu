package call

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/asr"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/expert"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/minio"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/prometheus"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/telc"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/utils"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/voice"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

const (
	IvaCallTo = "mcinext"
)

// Static errors for call service operations
var (
	ErrNotSessionType           = errors.New("message type is not 'session'")
	ErrMissingTrackPaths        = errors.New("missing required voice track paths")
	ErrFileDurationIssue        = fmt.Errorf("file with duration %d", FileWithIssueDurtion)
	ErrMissingTelCTimeData      = errors.New("missing TelC start_time or end_time and recovery is disabled")
	ErrDurationRecoveryDisabled = errors.New("duration is missing/invalid and recovery is disabled")
	ErrVoiceBufferNilOrEmpty    = errors.New("cannot recover duration: voice buffer is nil or empty")
	ErrCreatedAtNil             = errors.New("cannot recover time data: created_at is nil")
	ErrFileUpload               = errors.New("failed upload voice file to minio")
)

type CallMessage struct {
	CallID    string  `json:"call_id"`
	CallTo    string  `json:"call_to"`
	Duration  float64 `json:"duration"`
	Online    bool    `json:"online"`
	CreatedAt string  `json:"created_at"`
}

type IvaCallMessage struct {
	Type                 string  `json:"type"` // Message type, must be "session"
	CallID               string  `json:"call_id"`
	Duration             float64 `json:"session_duration"`
	ParticipantTrackPath string  `json:"participant_track_path"` // Path in Minio for participant voice
	AgentTrackPath       string  `json:"agent_track_path"`       // Path in Minio for agent voice
	AgentStartedTime     string  `json:"agent_started_time"`     // UTC timestamp when agent started
	AgentEndedTime       string  `json:"agent_ended_time"`       // UTC timestamp when agent ended
	PhoneNumber          string  `json:"phone_number"`           // Phone number from IVA platform
}

type CallResultMessage struct {
	Segments   []asr.Segment   `json:"segments"`
	Metadata   json.RawMessage `json:"metadata"`
	CallInfo   map[string]any  `json:"call_info"`
	Interrupts map[string]bool `json:"overlap"`
}

type CallService struct {
	CallRepository       *CallRepository
	ExpertRepository     *expert.ExpertRepository
	MinioIVAClient       *minio.MinioClient
	MinioCOAClient       *minio.MinioClient
	ASRClient            asr.TranscriptionProvider
	TelCService          *telc.TelCService
	VoiceMergeService    *voice.MergeService
	VoiceDurationService *voice.DurationService
}

func NewService(
	dbConn *gorm.DB,
	minioIVAClient *minio.MinioClient,
	minioCOAClient *minio.MinioClient,
	asrClient asr.TranscriptionProvider,
) *CallService {
	return &CallService{
		CallRepository:       NewCallRepository(dbConn),
		ExpertRepository:     expert.NewExpertRepository(dbConn),
		MinioIVAClient:       minioIVAClient,
		MinioCOAClient:       minioCOAClient,
		ASRClient:            asrClient,
		TelCService:          telc.NewService(),
		VoiceMergeService:    voice.NewMergeService(),
		VoiceDurationService: voice.NewDurationService(),
	}
}

func (callService *CallService) ProcessCallMessage(ctx context.Context, msg []byte) (*CallResultMessage, error) {
	var callMsg CallMessage

	err := json.Unmarshal(msg, &callMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal call message: %w", err)
	}

	err = callService.CallRepository.UpdateCallStatus(ctx, callMsg.CallID, StatusInProgress)
	if err != nil {
		return nil, err
	}

	var result *CallResultMessage

	if callMsg.Online {
		result, err = callService.processOnlineCall(ctx, &callMsg)
	} else {
		result, err = callService.processOfflineCall(ctx, &callMsg)
	}

	if err != nil {
		_ = callService.CallRepository.UpdateCallStatus(ctx, callMsg.CallID, StatusFailed)

		return nil, err
	}

	err = callService.CallRepository.UpdateCallStatus(ctx, callMsg.CallID, StatusSucceed)
	if err != nil {
		_ = callService.CallRepository.UpdateCallStatus(ctx, callMsg.CallID, StatusFailed)

		return nil, err
	}

	return result, nil
}

// ProcessIvaCallMessage processes IVA call messages with voice file merging
func (callService *CallService) ProcessIvaCallMessage(ctx context.Context, msg []byte) (*CallResultMessage, error) {
	var ivaCallMsg IvaCallMessage

	err := json.Unmarshal(msg, &ivaCallMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal IVA call message: %w", err)
	}

	// Only process messages with type "session"
	if ivaCallMsg.Type != "session" {
		logging.Logger.Debug("Skipping non-session message",
			zap.String("type", ivaCallMsg.Type),
			zap.String("call_id", ivaCallMsg.CallID),
		)

		return nil, fmt.Errorf("%w: got '%s'", ErrNotSessionType, ivaCallMsg.Type)
	}

	err = callService.CallRepository.UpdateCallStatus(ctx, ivaCallMsg.CallID, StatusInProgress)
	if err != nil {
		return nil, err
	}

	result, err := callService.processIvaCall(ctx, &ivaCallMsg)
	if err != nil {
		_ = callService.CallRepository.UpdateCallStatus(ctx, ivaCallMsg.CallID, StatusFailed)

		return nil, err
	}

	err = callService.CallRepository.UpdateCallStatus(ctx, ivaCallMsg.CallID, StatusSucceed)
	if err != nil {
		_ = callService.CallRepository.UpdateCallStatus(ctx, ivaCallMsg.CallID, StatusFailed)

		return nil, err
	}

	return result, nil
}

func (callService *CallService) processOnlineCall(
	ctx context.Context,
	callMsg *CallMessage,
) (*CallResultMessage, error) {
	callRecord, err := callService.CallRepository.GetTelcCallRecordByID(ctx, callMsg.CallID)
	if err != nil {
		logging.Logger.Error("Failed to fetch telc call record",
			zap.String("call_id", callMsg.CallID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	logging.Logger.Info(
		"Processing TelC Call record",
		zap.String("call_id", callMsg.CallID),
		zap.String("created_at", callRecord.CreatedAt.String()),
		zap.String("start_processing", time.Now().String()),
	)

	group, _ := errgroup.WithContext(ctx)

	group.Go(func() error { return callService.ensureExpertExists(ctx, callRecord) })

	var voiceBuffer *bytes.Buffer

	group.Go(func() error {
		buf, err := callService.uploadVoiceFile(ctx, callRecord)
		voiceBuffer = buf

		return err
	})

	err = group.Wait()
	if err != nil && !errors.Is(err, ErrFileDurationIssue) {
		logging.Logger.Error("Failed to process call",
			zap.String("call_id", callRecord.CallID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	// Update TelC call info after we have the voice buffer (needed for duration recovery)
	err = callService.updateTelcCallInfo(ctx, callRecord, voiceBuffer)
	if err != nil {
		logging.Logger.Error("Failed to update TelC call info",
			zap.String("call_id", callRecord.CallID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	asrResponse, err := callService.ASRClient.GetVoiceTranscriptions(ctx, voiceBuffer, callRecord.CallID)
	if err != nil {
		logging.Logger.Error("Failed to get voice asr",
			zap.String("call_id", callRecord.CallID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	logging.Logger.Info("voice file transacription fetched successfully", zap.String("call_id", callRecord.CallID))

	return &CallResultMessage{
		Segments: asrResponse.Segments,
		Metadata: asrResponse.Metadata,
		CallInfo: map[string]any{
			"call_id":         callRecord.CallID,
			"access_code":     callRecord.AccessCode,
			"city_code":       callRecord.CityCode,
			"service_no":      callRecord.ServiceNo,
			"call_start_date": utils.TimeToString(callRecord.CallEndDate),
			"call_end_date":   utils.TimeToString(callRecord.CallEndDate),
			"call_to":         callRecord.CallTo,
			"duration":        callRecord.Duration,
			"reasons":         callRecord.Reasons,
			"created_at":      callRecord.CreatedAt.String(),
		},
		Interrupts: voice.CheckChannelInterrupts(asrResponse.Segments, config.Conf.VoiceOverlapThreshold),
	}, nil
}

func (callService *CallService) processOfflineCall(
	ctx context.Context,
	callMsg *CallMessage,
) (*CallResultMessage, error) {
	voiceBuffer, err := callService.MinioCOAClient.Download(ctx, callMsg.CallID)
	if err != nil {
		return nil, err
	}

	logging.Logger.Info("voice file downloade from minio successfully", zap.String("call_id", callMsg.CallID))

	asrResponse, err := callService.ASRClient.GetVoiceTranscriptions(ctx, voiceBuffer, callMsg.CallID)
	if err != nil {
		return nil, err
	}

	logging.Logger.Info("voice file transacription fetched successfully", zap.String("call_id", callMsg.CallID))

	return &CallResultMessage{
		Segments: asrResponse.Segments,
		Metadata: asrResponse.Metadata,
		CallInfo: map[string]any{
			"call_id":  callMsg.CallID,
			"call_to":  callMsg.CallTo,
			"duration": callMsg.Duration,
		},
		Interrupts: voice.CheckChannelInterrupts(asrResponse.Segments, config.Conf.VoiceOverlapThreshold),
	}, nil
}

func (callService *CallService) processIvaCall(
	ctx context.Context,
	ivaCallMsg *IvaCallMessage,
) (*CallResultMessage, error) {
	logging.Logger.Info("Processing IVA call - downloading voice files",
		zap.String("call_id", ivaCallMsg.CallID),
		zap.String("participant_track", ivaCallMsg.ParticipantTrackPath),
		zap.String("agent_track", ivaCallMsg.AgentTrackPath),
	)

	// Validate that both track paths are present
	if ivaCallMsg.ParticipantTrackPath == "" || ivaCallMsg.AgentTrackPath == "" {
		return nil, fmt.Errorf("%w: participant=%q, agent=%q",
			ErrMissingTrackPaths, ivaCallMsg.ParticipantTrackPath, ivaCallMsg.AgentTrackPath)
	}

	// Parse agent start and end times in UTC
	var agentStartTime, agentEndTime *time.Time
	if ivaCallMsg.AgentStartedTime != "" {
		parsedStart, err := time.Parse(time.RFC3339, ivaCallMsg.AgentStartedTime)
		if err != nil {
			logging.Logger.Error("Failed to parse agent_started_time",
				zap.String("call_id", ivaCallMsg.CallID),
				zap.String("agent_started_time", ivaCallMsg.AgentStartedTime),
				zap.String("error", err.Error()),
			)

			return nil, fmt.Errorf("failed to parse agent_started_time: %w", err)
		}

		agentStartTime = &parsedStart
	}

	if ivaCallMsg.AgentEndedTime != "" {
		parsedEnd, err := time.Parse(time.RFC3339, ivaCallMsg.AgentEndedTime)
		if err != nil {
			logging.Logger.Error("Failed to parse agent_ended_time",
				zap.String("call_id", ivaCallMsg.CallID),
				zap.String("agent_ended_time", ivaCallMsg.AgentEndedTime),
				zap.String("error", err.Error()),
			)

			return nil, fmt.Errorf("failed to parse agent_ended_time: %w", err)
		}

		agentEndTime = &parsedEnd
	}

	// Download both voice files from Minio concurrently
	participantBuffer, agentBuffer, err := callService.downloadVoiceTracks(ctx, ivaCallMsg)
	if err != nil {
		return nil, err
	}

	// Merge and process the voice files
	voiceBuffer, err := callService.mergeIVAVoice(ctx, participantBuffer, agentBuffer, ivaCallMsg)
	if err != nil {
		return nil, err
	}

	return callService.processIVAVoice(ctx, voiceBuffer, ivaCallMsg, agentStartTime, agentEndTime)
}

func (callService *CallService) ensureExpertExists(ctx context.Context, callRecord *TelCCallRecord) error {
	_, err := callService.ExpertRepository.GetExpertByUserCode(ctx, callRecord.CallTo)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_, err = callService.ExpertRepository.CreateExpert(ctx, callRecord.CallTo)
	}

	return err
}

func (callService *CallService) updateTelcCallInfo(
	ctx context.Context,
	callRecord *TelCCallRecord,
	voiceBuffer *bytes.Buffer,
) error {
	info, err := callService.TelCService.GetCallInfo(ctx, callRecord.CallID)
	if err != nil {
		return err
	}

	startTime := info.StartTime
	endTime := info.EndTime

	// Recover duration if missing or invalid
	recoveredDuration, err := callService.recoverDurationIfNeeded(ctx, callRecord, voiceBuffer)
	if err != nil {
		return err
	}

	// Recover time data if missing
	startTime, endTime, err = callService.recoverTimeDataIfNeeded(callRecord, startTime, endTime)
	if err != nil {
		return err
	}

	// Update the record in the database
	updates := map[string]any{
		"reasons":         info.Reasons,
		"call_start_date": startTime,
		"call_end_date":   endTime,
	}

	// If duration was recovered, update it in the database as well
	if recoveredDuration {
		updates["duration"] = callRecord.Duration
	}

	return callService.CallRepository.UpdateTelCCallRecordWithUpdates(
		ctx,
		callRecord,
		updates,
	)
}

// recoverDurationIfNeeded checks if duration recovery is needed and performs it
func (callService *CallService) recoverDurationIfNeeded(
	ctx context.Context,
	callRecord *TelCCallRecord,
	voiceBuffer *bytes.Buffer,
) (bool, error) {
	// Check if duration is valid
	if callRecord.Duration != 0 && callRecord.Duration != FileWithIssueDurtion {
		return false, nil
	}

	// Duration is missing or invalid
	prometheus.TelcMissingDataTotal.WithLabelValues("duration").Inc()

	if !config.Conf.RecoverTelcData {
		logging.Logger.Warn("TelC duration is missing/invalid and recovery is disabled, dropping call",
			zap.String("call_id", callRecord.CallID),
			zap.Int("duration", callRecord.Duration),
		)

		return false, fmt.Errorf("%w: %d", ErrDurationRecoveryDisabled, callRecord.Duration)
	}

	// Recovery is enabled, extract duration from voice file
	return callService.extractDurationFromVoiceFile(ctx, callRecord, voiceBuffer)
}

// extractDurationFromVoiceFile extracts duration from the voice buffer
func (callService *CallService) extractDurationFromVoiceFile(
	ctx context.Context,
	callRecord *TelCCallRecord,
	voiceBuffer *bytes.Buffer,
) (bool, error) {
	if voiceBuffer == nil || voiceBuffer.Len() == 0 {
		logging.Logger.Error("Cannot recover duration: voice buffer is nil or empty",
			zap.String("call_id", callRecord.CallID),
		)

		return false, ErrVoiceBufferNilOrEmpty
	}

	logging.Logger.Info("TelC duration missing/invalid, extracting from voice file",
		zap.String("call_id", callRecord.CallID),
		zap.Int("original_duration", callRecord.Duration),
	)

	// Create a copy of the buffer to avoid consuming it
	bufferCopy := bytes.NewBuffer(voiceBuffer.Bytes())

	extractedDuration, err := callService.VoiceDurationService.GetDuration(ctx, bufferCopy, callRecord.CallID)
	if err != nil {
		logging.Logger.Error("Failed to extract duration from voice file",
			zap.String("call_id", callRecord.CallID),
			zap.String("error", err.Error()),
		)

		return false, fmt.Errorf("failed to extract duration: %w", err)
	}

	// Update the duration in the record
	callRecord.Duration = extractedDuration

	logging.Logger.Info("Duration recovered from voice file",
		zap.String("call_id", callRecord.CallID),
		zap.Int("recovered_duration", extractedDuration),
	)

	return true, nil
}

// recoverTimeDataIfNeeded checks if time data recovery is needed and performs it
func (callService *CallService) recoverTimeDataIfNeeded(
	callRecord *TelCCallRecord,
	startTime, endTime *time.Time,
) (*time.Time, *time.Time, error) {
	// Check if both times are present
	if startTime != nil && endTime != nil {
		return startTime, endTime, nil
	}

	// Time data is missing - record metrics
	if startTime == nil {
		prometheus.TelcMissingDataTotal.WithLabelValues("start_time").Inc()
	}

	if endTime == nil {
		prometheus.TelcMissingDataTotal.WithLabelValues("end_time").Inc()
	}

	if !config.Conf.RecoverTelcData {
		logging.Logger.Warn("TelC time data missing and recovery is disabled, dropping call",
			zap.String("call_id", callRecord.CallID),
			zap.Bool("start_time_present", startTime != nil),
			zap.Bool("end_time_present", endTime != nil),
		)

		return nil, nil, ErrMissingTelCTimeData
	}

	// Recovery is enabled, calculate missing times
	return callService.calculateMissingTimes(callRecord, startTime, endTime)
}

// calculateMissingTimes calculates missing start or end times from created_at and duration
func (callService *CallService) calculateMissingTimes(
	callRecord *TelCCallRecord,
	startTime, endTime *time.Time,
) (*time.Time, *time.Time, error) {
	logging.Logger.Info("TelC time data missing, recovering from created_at and duration",
		zap.String("call_id", callRecord.CallID),
		zap.Bool("start_time_present", startTime != nil),
		zap.Bool("end_time_present", endTime != nil),
		zap.Int("duration", callRecord.Duration),
	)

	if callRecord.CreatedAt == nil {
		logging.Logger.Error("Cannot recover time data: created_at is nil",
			zap.String("call_id", callRecord.CallID),
		)

		return nil, nil, ErrCreatedAtNil
	}

	// start_time = created_at - duration - 1 minute
	// end_time = created_at - 1 minute
	recoveredEndTime := callRecord.CreatedAt.Add(-1 * time.Minute)
	recoveredStartTime := recoveredEndTime.Add(-time.Duration(callRecord.Duration) * time.Second)

	if startTime == nil {
		startTime = &recoveredStartTime
		logging.Logger.Info("Recovered start_time",
			zap.String("call_id", callRecord.CallID),
			zap.String("recovered_start_time", startTime.Format(time.RFC3339)),
		)
	}

	if endTime == nil {
		endTime = &recoveredEndTime
		logging.Logger.Info("Recovered end_time",
			zap.String("call_id", callRecord.CallID),
			zap.String("recovered_end_time", endTime.Format(time.RFC3339)),
		)
	}

	return startTime, endTime, nil
}

func (callService *CallService) uploadVoiceFile(
	ctx context.Context,
	callRecord *TelCCallRecord,
) (*bytes.Buffer, error) {
	if callRecord.Duration == FileWithIssueDurtion {
		return nil, ErrFileDurationIssue
	}

	buffer, err := callService.TelCService.GetVoiceCall(ctx, callRecord.CallID)
	if err != nil {
		return nil, err
	}

	_, err = callService.MinioCOAClient.Upload(ctx, buffer, callRecord.CallID)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (callService *CallService) downloadVoiceTracks(
	ctx context.Context,
	ivaCallMsg *IvaCallMessage,
) (*bytes.Buffer, *bytes.Buffer, error) {
	group, groupCtx := errgroup.WithContext(ctx)

	var participantBuffer, agentBuffer *bytes.Buffer

	group.Go(func() error {
		buffer, err := callService.MinioIVAClient.Download(groupCtx, ivaCallMsg.ParticipantTrackPath)
		if err != nil {
			logging.Logger.Error("Failed to download participant track",
				zap.String("call_id", ivaCallMsg.CallID),
				zap.String("path", ivaCallMsg.ParticipantTrackPath),
				zap.String("error", err.Error()),
			)

			return fmt.Errorf("failed to download participant track: %w", err)
		}

		participantBuffer = buffer

		return nil
	})

	group.Go(func() error {
		buffer, err := callService.MinioIVAClient.Download(groupCtx, ivaCallMsg.AgentTrackPath)
		if err != nil {
			logging.Logger.Error("Failed to download agent track",
				zap.String("call_id", ivaCallMsg.CallID),
				zap.String("path", ivaCallMsg.AgentTrackPath),
				zap.String("error", err.Error()),
			)

			return fmt.Errorf("failed to download agent track: %w", err)
		}

		agentBuffer = buffer

		return nil
	})

	err := group.Wait()
	if err != nil {
		return nil, nil, err
	}

	logging.Logger.Info("Voice files downloaded successfully, merging...",
		zap.String("call_id", ivaCallMsg.CallID),
	)

	return participantBuffer, agentBuffer, nil
}

func (callService *CallService) mergeIVAVoice(
	ctx context.Context,
	participantBuffer *bytes.Buffer,
	agentBuffer *bytes.Buffer,
	ivaCallMsg *IvaCallMessage,
) (*bytes.Buffer, error) {
	// Merge the two voice files (participant and agent tracks)
	mergedVoiceBuffer, err := callService.VoiceMergeService.MergeVoiceFiles(
		ctx, participantBuffer, agentBuffer, ivaCallMsg.CallID,
	)
	if err != nil {
		logging.Logger.Error("Failed to merge voice files",
			zap.String("call_id", ivaCallMsg.CallID),
			zap.String("error", err.Error()),
		)

		return nil, fmt.Errorf("failed to merge voice files: %w", err)
	}

	_, err = callService.MinioCOAClient.Upload(ctx, mergedVoiceBuffer, ivaCallMsg.CallID)
	if err != nil {
		logging.Logger.Error("Failed to upload merged voice file to minio",
			zap.String("call_id", ivaCallMsg.CallID),
			zap.String("error", err.Error()),
		)

		return nil, ErrFileUpload
	}

	logging.Logger.Info("Voice files merged successfully, sending to ASR",
		zap.String("call_id", ivaCallMsg.CallID),
	)

	return mergedVoiceBuffer, nil
}

func (callService *CallService) processIVAVoice(
	ctx context.Context,
	voiceBuffer *bytes.Buffer,
	ivaCallMsg *IvaCallMessage,
	agentStartTime *time.Time,
	agentEndTime *time.Time,
) (*CallResultMessage, error) {
	// Process the merged voice through ASR (same as previous flow)
	asrResponse, err := callService.ASRClient.GetVoiceTranscriptions(
		ctx, voiceBuffer, ivaCallMsg.CallID,
	)
	if err != nil {
		logging.Logger.Error("Failed to get voice transcription",
			zap.String("call_id", ivaCallMsg.CallID),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	logging.Logger.Info("Voice transcription fetched successfully",
		zap.String("call_id", ivaCallMsg.CallID),
	)

	callInfo := map[string]any{
		"call_id":                ivaCallMsg.CallID,
		"call_to":                IvaCallTo,
		"duration":               ivaCallMsg.Duration,
		"participant_track_path": ivaCallMsg.ParticipantTrackPath,
		"agent_track_path":       ivaCallMsg.AgentTrackPath,
		"platform":               "iva",
		"type":                   ivaCallMsg.Type,
		"service_no":             ivaCallMsg.PhoneNumber,
		"access_code":            "9990",
		"city_code":              "unknown",
	}

	// Load Asia/Tehran timezone for IVA call times
	tehranLoc, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		logging.Logger.Error("Failed to load Asia/Tehran timezone",
			zap.String("call_id", ivaCallMsg.CallID),
			zap.String("error", err.Error()),
		)

		return nil, fmt.Errorf("failed to load Asia/Tehran timezone: %w", err)
	}

	if agentStartTime != nil {
		tehranStartTime := agentStartTime.In(tehranLoc)
		logging.Logger.Info("Converting agent start time to Tehran timezone",
			zap.String("call_id", ivaCallMsg.CallID),
			zap.String("utc_time", agentStartTime.Format(time.RFC3339)),
			zap.String("tehran_time", tehranStartTime.Format(time.RFC3339)),
		)
		callInfo["call_start_date"] = utils.TimeToString(&tehranStartTime)
	}

	if agentEndTime != nil {
		tehranEndTime := agentEndTime.In(tehranLoc)
		logging.Logger.Info("Converting agent end time to Tehran timezone",
			zap.String("call_id", ivaCallMsg.CallID),
			zap.String("utc_time", agentEndTime.Format(time.RFC3339)),
			zap.String("tehran_time", tehranEndTime.Format(time.RFC3339)),
		)
		callInfo["call_end_date"] = utils.TimeToString(&tehranEndTime)
	}

	return &CallResultMessage{
		Segments: asrResponse.Segments,
		Metadata: asrResponse.Metadata,
		CallInfo: callInfo,
		Interrupts: voice.CheckChannelInterrupts(
			asrResponse.Segments, config.Conf.VoiceOverlapThreshold,
		),
	}, nil
}
