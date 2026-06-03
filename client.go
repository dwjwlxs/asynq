package asynq

import (
	"context"

	"github.com/hibiken/asynq"
)

type Option = asynq.Option

type Task struct {
	*asynq.Task
	opts []Option
}

func NewTask(typename string, payload []byte, headers map[string]string, opts ...Option) *Task {
	return &Task{
		Task: asynq.NewTaskWithHeaders(typename, payload, headers, opts...),
		opts: opts,
	}
}

func (t *Task) Opts() []Option {
	return t.opts
}

type Client interface {
	Ping() error
	EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*asynq.TaskInfo, error)
}

func NewClient(conf Config) Client {
	return &clientImpl{
		asynqClient: asynq.NewClient(asynq.RedisClientOpt{
			Addr:     conf.RedisURL,
			PoolSize: conf.PoolSize,
		}),
	}
}

type clientImpl struct {
	asynqClient *asynq.Client
}

func (c *clientImpl) Ping() error {
	return c.asynqClient.Ping()
}

func (c *clientImpl) EnqueueContext(ctx context.Context, task *Task, opts ...Option) (*asynq.TaskInfo, error) {
	return c.asynqClient.EnqueueContext(ctx, task.Task, opts...)
}
