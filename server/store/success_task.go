package store

import (
	"time"

	"github.com/go-pg/pg/orm"
)

// SuccessTask 成功的任务
type SuccessTask struct {
	tableName struct{} `sql:"?schema.success_tasks" pg:",discard_unknown_columns"`

	ID          int64
	Group       string
	OriginalID  string
	Name        string
	Params      []byte
	ScheduleAt  int64
	RetryTime   int64
	NumRetried  int64
	Result      []byte
	CreatedTime time.Time
}

// SuccessTaskDao 成功任务DAO
type SuccessTaskDao struct {
	orm.DB
}
