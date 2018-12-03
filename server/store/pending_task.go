package store

import (
	"fmt"
	"time"

	"github.com/DrWrong/scheduler/proto"
	"github.com/DrWrong/scheduler/server/global"
	"github.com/go-pg/pg/orm"
	"github.com/sirupsen/logrus"
)

// 任务状态
const (
	TaskStatusPending   = "pending"
	TaskStatusDelivered = "delivered"
)

// Task pending状态的任务
type Task struct {
	tableName struct{} `sql:"?schema.pending_tasks" pg:",discard_unknown_columns"`

	ID           int64
	Topic        string
	OriginalID   string
	Name         string
	Payload      []byte
	ScheduleAt   int64
	MaxRetryTime int64
	RetriedTime  int64
	Status       string
	DeliveredAt  int64
	CreatedTime  time.Time
	Lastupdated  time.Time
}

// ToDomain 转成Domain对象
func (t *Task) ToDomain() *proto.Task {
	return &proto.Task{
		Id:           fmt.Sprintf("%d", t.ID),
		Topic:        t.Topic,
		OriginalID:   t.OriginalID,
		Name:         t.Name,
		Payload:      t.Payload,
		ScheduleAt:   t.ScheduleAt,
		MaxRetryTime: t.MaxRetryTime,
		RetriedTime:  t.RetriedTime,
	}
}

// TaskDao pending task data access object
type TaskDao struct {
	orm.DB
}

// FetchPendingTasks 拉取数据
func (d *TaskDao) FetchPendingTasks() ([]*Task, error) {
	var tasks []*Task
	if err := d.Model(&tasks).Where("status = ?", TaskStatusPending).Order("schedule_at").Limit(global.Config.TaskFetcher.DBFetchTaskNumber).Select(); err != nil {
		logrus.WithField("err", err).Error("Fetch task from db error")
		return nil, err
	}
	return tasks, nil
}

// FetchDeliveredTasks 重放
func (d *TaskDao) FetchDeliveredTasks(deliverTaskReplyDuration time.Duration) ([]*Task, error) {
	var tasks []*Task
	if err := d.Model(&tasks).Where("status = ?", TaskStatusDelivered).
		Where("delivered_at <= ?", time.Now().Add(-deliverTaskReplyDuration).UnixNano()).
		Limit(global.Config.TaskFetcher.DBFetchTaskNumber).
		Select(); err != nil {
		logrus.WithField("err", err).Error("Fetch delivered tasks error")
		return nil, err
	}
	return tasks, nil
}

// FindTaskByID Find task by id
func (d *TaskDao) FindTaskByID(id int64) (*Task, error) {
	task := new(Task)
	if err := d.Model(&task).Where("id = ?", id).Select(); err != nil {
		logrus.WithField("err", err).Error("Find task by id error")
		return nil, err
	}
	return task, nil
}

// BatchInsertTasks 批量插入数据
func (d *TaskDao) BatchInsertTasks(tasks *[]*Task) error {
	_, err := d.Model(tasks).OnConflict("DO NOTHING").Returning("id", "name", "payload", "schedule_at", "max_retry_time", "retried_time").Insert()
	return err
}

// Update 更新
func (d *TaskDao) Update(task *Task, columns ...string) error {
	task.Lastupdated = time.Now()
	if len(columns) > 0 {
		columns = append(columns, "lastupdated")
	}
	if _, err := d.Model(task).Column(columns...).WherePK().Update(); err != nil {
		return err
	}

	return nil
}
