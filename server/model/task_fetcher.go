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
	TaskGroup string

	queue         pendingTaskTimeQueue
	locker        sync.Mutex
	lastEmptyTime time.Time
	lastFetchTime time.Time
}

func (f *TaskFetcher) suplyQueueIfNeeded() error {
	// 超过5min后直接拉取
	if time.Now().Sub(f.lastFetchTime) < 5*time.Minute && f.queue.Len() > 0 {
		return nil
	}

	if time.Now().Sub(f.lastEmptyTime) < time.Second {
		logrus.WithField("taskGroup", f.TaskGroup).Info("Task queue is emptry sleep for a while")
		return nil
	}
	logrus.WithField("taskGroup", f.TaskGroup).Debug("Going to load task queue for group")
	dao := store.TaskDao{
		DB: global.GetDBByTaskGroup(f.TaskGroup),
	}
	tasks, err := dao.FetchTasks()
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
		logrus.WithField("taskGroup", f.TaskGroup).WithField("err", err).Error("supply queue error")
		return nil, err
	}

	if f.queue.Len() == 0 {
		return nil, nil
	}
	lastItem := f.queue[f.queue.Len()-1]
	if lastItem.ScheduleAt >= scheduleTime {
		return nil, nil
	}
	heap.Pop(&f.queue)
	logrus.WithField("taskGroup", f.TaskGroup).WithField("task", lastItem).Info("Popup task")
	return lastItem, nil
}

// AddTasks 添加任务
func (f *TaskFetcher) AddTasks(tasks []*store.Task) error {
	f.locker.Lock()
	defer f.locker.Unlock()

	taskDao := store.TaskDao{DB: global.GetDBByTaskGroup(f.TaskGroup)}

	if err := taskDao.Insert(&tasks); err != nil {
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

	if err := global.GetDBByTaskGroup(f.TaskGroup).RunInTransaction(func(tx *pg.Tx) error {
		taskDao := store.TaskDao{DB: tx}
		task, err := taskDao.FindTaskByID(taskID)
		if err != nil {
			return err
		}

		successTaskDao := store.SuccessTaskDao{DB: tx}
		if err := successTaskDao.Insert(&store.SuccessTask{
			ID:          task.ID,
			Group:       task.Group,
			OriginalID:  task.OriginalID,
			Name:        task.Name,
			Params:      task.Params,
			ScheduleAt:  task.ScheduleAt,
			RetryTime:   task.RetryTime,
			NumRetried:  task.NumRetried,
			Result:      result,
			CreatedTime: time.Now(),
		}); err != nil {
			return err
		}

		if err := taskDao.Delete(task); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// AckFail ack fail
func (f *TaskFetcher) AckFail(taskID int64, addRetryTime bool) error {
	f.locker.Lock()
	defer f.locker.Unlock()

	var retryTask *store.Task
	if err := global.GetDBByTaskGroup(f.TaskGroup).RunInTransaction(func(tx *pg.Tx) error {
		taskDao := store.TaskDao{DB: tx}
		task, err := taskDao.FindTaskByID(taskID)
		if err != nil {
			return err
		}

		if task.NumRetried < task.RetryTime {
			if addRetryTime {
				task.NumRetried++
			}
			task.ScheduleAt = time.Now().Add(time.Second).UnixNano()
			if err := taskDao.Update(task, "num_retried", "schedule_at"); err != nil {
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
			ID:          task.ID,
			Group:       task.Group,
			OriginalID:  task.OriginalID,
			Name:        task.Name,
			Params:      task.Params,
			ScheduleAt:  task.ScheduleAt,
			RetryTime:   task.RetryTime,
			CreatedTime: time.Now(),
		})

		return nil

	}); err != nil {
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
