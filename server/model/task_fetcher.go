package model

import (
	"container/heap"
	"sync"
	"time"

	"github.com/DrWrong/scheduler/server/global"
	"github.com/DrWrong/scheduler/server/store"
	"github.com/go-pg/pg"
	"github.com/sirupsen/logrus"
)

// 按时间排列的最小树
type pendingTaskTimeQueue []*store.Task

func (p pendingTaskTimeQueue) Len() int {
	return len(p)
}

func (p pendingTaskTimeQueue) Less(i, j int) bool {
	return p[i].ScheduleAt < p[j].ScheduleAt
}

func (p pendingTaskTimeQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *pendingTaskTimeQueue) Push(x interface{}) {
	task := x.(*store.Task)
	*p = append(*p, task)
}

func (p *pendingTaskTimeQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	*p = old[0 : n-1]
	return item
}

// TaskFetcher 取任务
type TaskFetcher struct {
	Topic       string
	SchedulerID string

	queue         pendingTaskTimeQueue
	locker        sync.Mutex
	lastEmptyTime time.Time
	lastFetchTime time.Time

	logger         *logrus.Entry
	loggerInitOnce sync.Once
}

func (f *TaskFetcher) getLogger() *logrus.Entry {
	f.loggerInitOnce.Do(func() {
		f.logger = logrus.WithField("topic", f.Topic)
	})

	return f.logger
}

func (f *TaskFetcher) fetchTasksFromDB() ([]*store.Task, error) {
	dao := store.TaskDao{
		DB: global.GetDBByTaskTopic(f.Topic),
	}

	pendingTasks, err := dao.FetchPendingTasks()
	if err != nil {
		return nil, err
	}

	deliveredTasks, err := dao.FetchDeliveredTasks(global.Config.TaskFetcher.DeliverTaskReplyDuration)
	if err != nil {
		return nil, err
	}

	return append(deliveredTasks, pendingTasks...), nil
}

func (f *TaskFetcher) suplyQueueIfNeeded() error {
	// cache是否在有效期内
	if time.Now().Sub(f.lastFetchTime) < global.Config.TaskFetcher.CacheMaxDuration && f.queue.Len() > 0 {
		return nil
	}

	// 空的队列是否超过最小等待时间
	if time.Now().Sub(f.lastEmptyTime) < global.Config.TaskFetcher.EmptyMinDuration {
		f.getLogger().Info("Task queue is emptry sleep for a while")
		return nil
	}

	f.getLogger().Debug("Going to load task queue for group")

	tasks, err := f.fetchTasksFromDB()
	if err != nil {
		return err
	}

	if len(tasks) == 0 {
		f.lastEmptyTime = time.Now()
	} else {
		f.queue = pendingTaskTimeQueue(tasks)
		heap.Init(&f.queue)
		f.lastFetchTime = time.Now()
	}

	return nil

}

// FetchTask 取可以执行的数据
func (f *TaskFetcher) FetchTask(scheduleTime int64) (*store.Task, error) {
	f.locker.Lock()
	defer f.locker.Unlock()
	if err := f.suplyQueueIfNeeded(); err != nil {
		f.getLogger().WithField("err", err).Error("supply queue error")
		return nil, err
	}

	if f.queue.Len() == 0 {
		f.getLogger().Debug("No task is the queue")
		return nil, nil
	}

	lastItem := f.queue[f.queue.Len()-1]
	if lastItem.ScheduleAt >= scheduleTime {
		f.getLogger().Debug("No task is at time to execute")
		return nil, nil
	}
	lastItem.Status = store.TaskStatusDelivered
	lastItem.DeliveredAt = time.Now().UnixNano()
	dao := store.TaskDao{
		DB: global.GetDBByTaskTopic(f.Topic),
	}
	if err := dao.Update(lastItem, "status", "delivered_at"); err != nil {
		return nil, err
	}
	heap.Pop(&f.queue)
	f.getLogger().WithField("task", lastItem).Info("Popup task")
	return lastItem, nil
}

// AddTasks 添加任务
func (f *TaskFetcher) AddTasks(tasks []*store.Task) error {
	f.locker.Lock()
	defer f.locker.Unlock()

	taskDao := store.TaskDao{DB: global.GetDBByTaskTopic(f.Topic)}

	if err := taskDao.BatchInsertTasks(&tasks); err != nil {
		return err
	}

	if f.queue == nil {
		f.queue = make(pendingTaskTimeQueue, 0)
	}
	for _, task := range tasks {
		heap.Push(&f.queue, task)

	}
	return nil

}

// AckSuccess ack success
func (f *TaskFetcher) AckSuccess(taskID int64, result []byte) error {
	f.locker.Lock()
	defer f.locker.Unlock()

	if err := global.GetDBByTaskTopic(f.Topic).RunInTransaction(func(tx *pg.Tx) error {
		taskDao := store.TaskDao{DB: tx}
		task, err := taskDao.FindTaskByID(taskID)
		if err != nil {
			return err
		}

		successTaskDao := store.SuccessTaskDao{DB: tx}
		if err := successTaskDao.Insert(&store.SuccessTask{
			ID:           task.ID,
			Topic:        task.Topic,
			OriginalID:   task.OriginalID,
			Name:         task.Name,
			Payload:      task.Payload,
			ScheduleAt:   task.ScheduleAt,
			MaxRetryTime: task.MaxRetryTime,
			RetriedTime:  task.RetriedTime,
			Result:       result,
			CreatedTime:  time.Now(),
		}); err != nil {
			return err
		}

		if err := taskDao.Delete(task); err != nil {
			return err
		}

		return nil
	}); err != nil {
		f.getLogger().WithField("err", err).Error("Ack success error")
		return err
	}

	return nil
}

// AckFail ack fail
func (f *TaskFetcher) AckFail(taskID int64, addRetryTime bool, scheduleAt int64) error {
	f.locker.Lock()
	defer f.locker.Unlock()

	var retryTask *store.Task
	if err := global.GetDBByTaskTopic(f.Topic).RunInTransaction(func(tx *pg.Tx) error {
		taskDao := store.TaskDao{DB: tx}
		task, err := taskDao.FindTaskByID(taskID)
		if err != nil {
			return err
		}

		if task.RetriedTime < task.MaxRetryTime {
			if addRetryTime {
				task.RetriedTime++
			}

			task.ScheduleAt = scheduleAt
			if task.ScheduleAt == 0 {
				task.ScheduleAt = time.Now().Add(time.Second).UnixNano()
			}

			task.Status = store.TaskStatusPending
			if err := taskDao.Update(task, "status", "num_retried", "schedule_at"); err != nil {
				return err
			}
			retryTask = task
			return nil
		}

		if err := taskDao.Delete(task); err != nil {
			return err
		}
		failTaskDao := store.FailTaskDao{DB: tx}
		failTaskDao.Insert(&store.FailTask{
			ID:           task.ID,
			Topic:        task.Topic,
			OriginalID:   task.OriginalID,
			Name:         task.Name,
			Payload:      task.Payload,
			ScheduleAt:   task.ScheduleAt,
			MaxRetryTime: task.MaxRetryTime,
			RetriedTime:  task.RetriedTime,
			CreatedTime:  time.Now(),
		})

		return nil

	}); err != nil {
		f.getLogger().WithField("err", err).Error("Ack failed error")
		return err
	}
	if retryTask != nil {
		if f.queue == nil {
			f.queue = make(pendingTaskTimeQueue, 0)
		}
		heap.Push(&f.queue, retryTask)
	}

	return nil
}
