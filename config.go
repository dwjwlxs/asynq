package asynq

import (
	"github.com/bytedance/gopkg/util/logger"
	"github.com/hibiken/asynq"
)

type Config struct {
	RedisURL     string
	Concurrency  int
	PoolSize     int
	TimeZone     string
	QueuesConfig map[string]int

	DisableCronJobEmitter bool

	Logger logger.Logger

	Middlewares []asynq.MiddlewareFunc
}
