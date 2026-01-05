package main

import (
	"context"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/grogu"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/logging"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/prometheus"
	"go.uber.org/zap"
)

func main() {
	go prometheus.Run()

	for {
		ctx, cancel := context.WithCancel(context.Background())

		app, err := grogu.NewApp(cancel)
		if err != nil {
			logging.Logger.Fatal("failed to create grogu app", zap.String("error", err.Error()))
		}

		err = app.Run(ctx)
		if err != nil {
			panic(err)
		}

		<-ctx.Done()

		app.HealthCheckerService.Check()

		cancel()
	}
}
