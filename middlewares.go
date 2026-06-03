package asynq

import (
	"context"
	"fmt"

	"github.com/hibiken/asynq"
)

func PanicRecovery() asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("%v", r)
				}
			}()
			return next.ProcessTask(ctx, t)
		})
	}
}
