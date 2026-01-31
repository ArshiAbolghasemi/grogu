package healthchecker

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/circuitbreak"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/config"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/minio"
	"go.uber.org/zap"
)

var testFileKey = "test.wav"

func CheckMinioCOA() error {
	return checkMinio(
		config.Conf.MinioAccessKey,
		config.Conf.MinioSecretKey,
		config.Conf.MinioBucketName,
		config.Conf.MinioPathPrefix,
		circuitbreak.MinioCOAService,
	)
}

func CheckMinioIVA() error {
	return checkMinio(
		config.Conf.MinioIvaAccessKey,
		config.Conf.MinioIvaSecretKey,
		config.Conf.MinioIvaBucketName,
		config.Conf.MinioIvaPathPrefix,
		circuitbreak.MinioIVAService,
	)
}

func checkMinio(accessKey, secretKey, bucketName, pathPrefix, service string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	minioClient, err := minio.NewMinioClient(accessKey, secretKey, bucketName, pathPrefix, service)
	if err != nil {
		logging.Logger.Error("failed to create new minio client", zap.String("error", err.Error()))
		return err
	}

	buf := readTestFile()

	_, err = minioClient.Upload(ctx, buf, testFileKey)
	if err != nil {
		return err
	}

	_, err = minioClient.Download(ctx, testFileKey)
	if err != nil {
		return err
	}

	return nil
}
