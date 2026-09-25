package logging

import (
	"go.uber.org/zap"
)

func GetLogger() *zap.SugaredLogger {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()

	return sugar
}
