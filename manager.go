package asynq

import (
	"context"
	"sync"

	"github.com/hibiken/asynq"
)

type HandleFunc func(context.Context, *Task) error

type Manager interface {
	// will launch goroutine, blocks until some specified `os.Signal`s received
	Run()

	RegisterHandler(name string, fn HandleFunc)

	// will set a job id which ensure cron triggers the job only once when time is up.
	// be careful when set a retention which should be end before the next turn,
	// otherwise the job will not be triggered in the next turn due to the conflict job id.
	RegisterCron(cronspec string, task *Task, opts ...Option) (entryID string, err error)
}

func NewManager(conf Config) Manager {
	schedConfig := &asynq.SchedulerOpts{
		PreEnqueueFunc: func(task *asynq.Task, opts []asynq.Option) {
			if task == nil {
				return
			}

			conf.Logger.CtxInfof(context.Background(), "event: %v, data: %+v", "asynq_scheduler_pre_enqueues", map[string]interface{}{
				"task_type": task.Type(),
				"opts":      opts,
				"payload":   string(task.Payload()),
			})
		},
		PostEnqueueFunc: func(task *asynq.TaskInfo, err error) {
			if err != nil {
				var taskId string
				var taskType string
				var payload string
				if task != nil {
					taskId = task.ID
					taskType = task.Type
					payload = string(task.Payload)
				}

				conf.Logger.CtxErrorf(context.Background(), "event: %v, data: %+v", "asynq_scheduler_post_enqueues", map[string]interface{}{
					"task_id":   taskId,
					"task_type": taskType,
					"payload":   payload,
				}, err)
			}
		},
	}
	servConfig := asynq.Config{
		Concurrency:    conf.Concurrency,
		StrictPriority: true,
	}
	if conf.QueuesConfig != nil {
		servConfig.Queues = conf.QueuesConfig
	}

	mux := asynq.NewServeMux()
	mux.Use(conf.Middlewares...)
	return &managerImpl{
		scheduler: asynq.NewScheduler(asynq.RedisClientOpt{Addr: conf.RedisURL}, schedConfig),

		mux:    mux,
		server: asynq.NewServer(asynq.RedisClientOpt{Addr: conf.RedisURL}, servConfig),

		config: conf,
	}
}

type managerImpl struct {
	scheduler *asynq.Scheduler

	mux    *asynq.ServeMux
	server *asynq.Server

	config Config
}

func (m *managerImpl) Run() {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.server.Run(m.mux)
	}()
	go func() {
		defer wg.Done()
		m.scheduler.Run()
	}()
	wg.Wait()
}

func (m *managerImpl) RegisterHandler(name string, fn HandleFunc) {
	asynqHandleFunc := func(ctx context.Context, task *asynq.Task) error {
		t := &Task{
			Task: task,
		}
		return fn(ctx, t)
	}
	m.mux.HandleFunc(name, asynqHandleFunc)
}

func (m *managerImpl) RegisterCron(cronspec string, task *Task, opts ...Option) (entryID string, err error) {
	opts = append(opts, asynq.TaskID(task.Type()))

	if m.config.DisableCronJobEmitter {
		m.config.Logger.CtxInfof(context.Background(), "event: %v, data: %+v", "asynq_turn_off_cron", map[string]interface{}{
			"task_type": task.Task.Type(),
		})
		return "", nil
	}
	return m.scheduler.Register(cronspec, task.Task, opts...)
}
