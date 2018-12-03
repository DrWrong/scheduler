package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/DrWrong/scheduler/proto"
	"github.com/DrWrong/scheduler/server/model"
	"github.com/DrWrong/scheduler/server/store"
	"github.com/sirupsen/logrus"
)

// SchedulerServer 调度服务器
type SchedulerServer struct {
	fetcherMap      map[string]*model.TaskFetcher
	fetcherLock     sync.Mutex
	groupChannelMap map[string]chan *store.Task
}

func newServer() *SchedulerServer {
	return &SchedulerServer{
		fetcherMap: make(map[string]*model.TaskFetcher),
	}
}

func (s *SchedulerServer) getFetcher(taskGroup string) *model.TaskFetcher {
	s.fetcherLock.Lock()
	defer s.fetcherLock.Unlock()
	fetcher, exist := s.fetcherMap[taskGroup]
	if exist {
		return fetcher
	}

	s.fetcherMap[taskGroup] = &model.TaskFetcher{
		Topic: taskGroup,
	}

	return s.fetcherMap[taskGroup]
}

// ScheduleTask 任务分发
func (s *SchedulerServer) ScheduleTask(ctx context.Context, req *proto.ScheduleTaskRequest) (*proto.ScheduleTaskResponse, error) {

	if len(req.BatchTasks) == 0 {
		return nil, errors.New("Batch task is 0")
	}

	fetcher := s.getFetcher(req.Topic)

	storeTasks := make([]*store.Task, 0, len(req.BatchTasks))
	for _, task := range req.BatchTasks {
		storeTasks = append(storeTasks, &store.Task{
			Topic:        task.Topic,
			OriginalID:   task.OriginalID,
			Name:         task.Name,
			Payload:      task.Payload,
			ScheduleAt:   task.ScheduleAt,
			MaxRetryTime: task.MaxRetryTime,
		})
	}

	if err := fetcher.AddTasks(storeTasks); err != nil {
		return nil, err
	}
	resp := new(proto.ScheduleTaskResponse)

	for _, task := range storeTasks {
		resp.BatchTasks = append(resp.BatchTasks, task.ToDomain())
	}
	return resp, nil
}

// FetchTask 任务分发
func (s *SchedulerServer) FetchTask(req *proto.FetchTaskRequest, stream proto.Scheduler_FetchTaskServer) error {
	logrus.WithField("Receive request to fetch task %+v", req)
	fetcher := s.getFetcher(req.Topic)

	var internalErrorTime int
	for {
		task, err := fetcher.FetchTask(time.Now().UnixNano())
		if err == nil {
			internalErrorTime = 0
			if task != nil {
				if err := stream.Send(task.ToDomain()); err != nil {
					fetcher.AckFail(task.ID, false, 0)
					logrus.WithField("err", err).Error("Send to client error")
					return err
				}

			}
		} else {
			logrus.WithField("err", err).Error("Fetch task internal error")
			internalErrorTime++
			if internalErrorTime > 5 {
				return err
			}
			time.Sleep(time.Second)
		}

	}
}

// AckTask 通知任务成功
func (s *SchedulerServer) AckTask(ctx context.Context, req *proto.TaskAck) (*proto.AckResponse, error) {
	fetcher := s.getFetcher(req.Topic)
	taskID, _ := strconv.ParseInt(req.Id, 10, 64)
	if req.Successed {
		err := fetcher.AckSuccess(taskID, req.Result)
		return nil, err
	}
	err := fetcher.AckFail(taskID, true, req.ScheduleAt)
	return nil, err

}
