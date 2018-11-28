package store

import (
	"time"

	"github.com/go-pg/pg/orm"
)

// FailTask 失败状态的任务
type FailTask struct {
	tableName struct{} `sql:"?schema.fail_tasks" pg:",discard_unknown_columns"`

	ID          int64
	Group       string
	OriginalID  string
	Name        string
	Params      []byte
	ScheduleAt  int64
	RetryTime   int64
	CreatedTime time.Time
}

// FailTaskDao 失败的任务
type FailTaskDao struct {
	orm.DB
}
