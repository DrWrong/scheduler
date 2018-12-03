package store

import (
	"time"

	"github.com/go-pg/pg/orm"
)

// SuccessTask 成功的任务
type SuccessTask struct {
	tableName struct{} `sql:"?schema.success_tasks" pg:",discard_unknown_columns"`

	ID           int64
	Topic        string
	OriginalID   string
	Name         string
	Payload      []byte
	ScheduleAt   int64
	MaxRetryTime int64
	RetriedTime  int64
	Result       []byte
	CreatedTime  time.Time
}

// SuccessTaskDao 成功任务DAO
type SuccessTaskDao struct {
	orm.DB
}
