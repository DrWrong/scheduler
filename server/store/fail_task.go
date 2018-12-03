package store

import (
	"time"

	"github.com/go-pg/pg/orm"
)

// FailTask 失败状态的任务
type FailTask struct {
	tableName struct{} `sql:"?schema.fail_tasks" pg:",discard_unknown_columns"`

	ID           int64
	Topic        string
	OriginalID   string
	Name         string
	Payload      []byte
	ScheduleAt   int64
	MaxRetryTime int64
	RetriedTime  int64
	CreatedTime  time.Time
}

// FailTaskDao 失败的任务
type FailTaskDao struct {
	orm.DB
}
