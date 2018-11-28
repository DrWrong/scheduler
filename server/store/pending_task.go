package store

import (
	"fmt"
	"time"

	"github.com/DrWrong/scheduler/proto"
	"github.com/go-pg/pg/orm"
	"github.com/sirupsen/logrus"
)

// Task pending状态的任务
type Task struct {
	tableName struct{} `sql:"?schema.pending_tasks" pg:",discard_unknown_columns"`

	ID          int64
	Group       string
	OriginalID  string
	Name        string
	Params      []byte
	ScheduleAt  int64
	RetryTime   int64
	NumRetried  int64
	CreatedTime time.Time
	Lastupdated time.Time
}

// ToDomain 转成Domain对象
func (t *Task) ToDomain() *proto.Task {
	return &proto.Task{
		TaskID:         fmt.Sprintf("%d", t.ID),
		TaskGroup:      t.Group,
		TaskOriginalID: t.OriginalID,
		TaskName:       t.Name,
		Params:         t.Params,
		ScheduleAt:     t.ScheduleAt,
		MaxRetryTime:   t.RetryTime,
	}
}

// TaskDao pending task data access object
type TaskDao struct {
	orm.DB
}

// FetchTasks 拉取数据
func (d *TaskDao) FetchTasks() ([]*Task, error) {
	var tasks []*Task
	if err := d.Model(&tasks).Order("schedule_at").Limit(1000).Select(); err != nil {
		logrus.WithField("err", err).Error("Fetch task from db error")
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
