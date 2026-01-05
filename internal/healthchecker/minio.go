package healthchecker

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/minio"
	"go.uber.org/zap"
)

var testFileKey = "test.wav"

func CheckMinio() bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	minioClient, err := minio.NewMinioClient()
	if err != nil {
		logging.Logger.Error("failed to create new minio client", zap.String("error", err.Error()))
		return false
	}

	buf := readTestFile()

	_, err = minioClient.Upload(ctx, buf, testFileKey)
	isUploadOk := (err == nil)

	_, err = minioClient.Download(ctx, testFileKey)
	isDownloadOk := (err == nil)

	return (isUploadOk && isDownloadOk)
}
