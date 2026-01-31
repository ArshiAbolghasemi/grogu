package healthchecker

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/asr"
)

var monitorCallID = "monitor_call_id"

func CheckASR() error {
	buffer := readTestFile()

	asrClient := asr.NewClient()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := asrClient.GetVoiceTranscriptions(ctx, buffer, monitorCallID)

	return err
}
