package asynq

import (
	"github.com/google/wire"
)

var AsynqSet = wire.NewSet(
	NewClient,
	NewManager,
)
