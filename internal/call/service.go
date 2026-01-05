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
	ErrNotSessionType    = errors.New("message type is not 'session'")
	ErrMissingTrackPaths = errors.New("missing required voice track paths")
	ErrFileDurationIssue = fmt.Errorf("file with duration %d", FileWithIssueDurtion)
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
}

type CallResultMessage struct {
	Segments   []asr.Segment   `json:"segments"`
	Metadata   json.RawMessage `json:"metadata"`
	CallInfo   map[string]any  `json:"call_info"`
	Interrupts map[string]bool `json:"overlap"`
}

type CallService struct {
	CallRepository    *CallRepository
	ExpertRepository  *expert.ExpertRepository
	MinioClient       *minio.MinioClient
	ASRClient         *asr.ASRClient
	TelCService       *telc.TelCService
	VoiceMergeService *voice.MergeService
}

func NewService(
	dbConn *gorm.DB,
	minioClient *minio.MinioClient,
	asrClient *asr.ASRClient,
) *CallService {
	return &CallService{
		CallRepository:    NewCallRepository(dbConn),
		ExpertRepository:  expert.NewExpertRepository(dbConn),
		MinioClient:       minioClient,
		ASRClient:         asrClient,
		TelCService:       telc.NewService(),
		VoiceMergeService: voice.NewMergeService(),
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
	group.Go(func() error { return callService.updateTelcCallInfo(ctx, callRecord) })

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
	voiceBuffer, err := callService.MinioClient.Download(ctx, callMsg.CallID)
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
	return callService.mergeAndProcessVoice(ctx, participantBuffer, agentBuffer, ivaCallMsg, agentStartTime, agentEndTime)
}

func (callService *CallService) ensureExpertExists(ctx context.Context, callRecord *TelCCallRecord) error {
	_, err := callService.ExpertRepository.GetExpertByUserCode(ctx, callRecord.CallTo)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		_, err = callService.ExpertRepository.CreateExpert(ctx, callRecord.CallTo)
	}

	return err
}

func (callService *CallService) updateTelcCallInfo(ctx context.Context, callRecord *TelCCallRecord) error {
	info, err := callService.TelCService.GetCallInfo(ctx, callRecord.CallID)
	if err != nil {
		return err
	}

	return callService.CallRepository.UpdateTelCCallRecord(
		ctx,
		callRecord,
		info.Reasons,
		info.StartTime,
		info.EndTime,
	)
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

	_, err = callService.MinioClient.Upload(ctx, buffer, callRecord.CallID)
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
		buffer, err := callService.MinioClient.Download(groupCtx, ivaCallMsg.ParticipantTrackPath)
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
		buffer, err := callService.MinioClient.Download(groupCtx, ivaCallMsg.AgentTrackPath)
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

func (callService *CallService) mergeAndProcessVoice(
	ctx context.Context,
	participantBuffer *bytes.Buffer,
	agentBuffer *bytes.Buffer,
	ivaCallMsg *IvaCallMessage,
	agentStartTime *time.Time,
	agentEndTime *time.Time,
) (*CallResultMessage, error) {
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

	logging.Logger.Info("Voice files merged successfully, sending to ASR",
		zap.String("call_id", ivaCallMsg.CallID),
	)

	// Process the merged voice through ASR (same as previous flow)
	asrResponse, err := callService.ASRClient.GetVoiceTranscriptions(
		ctx, mergedVoiceBuffer, ivaCallMsg.CallID,
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
	}

	if agentStartTime != nil {
		callInfo["call_start_date"] = utils.TimeToString(agentStartTime)
	}

	if agentEndTime != nil {
		callInfo["call_end_date"] = utils.TimeToString(agentEndTime)
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
