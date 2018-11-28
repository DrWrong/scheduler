package main

import (
	"flag"
	"net"

	"github.com/DrWrong/scheduler/proto"
	"github.com/DrWrong/scheduler/server/global"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", global.Config.GRPC.Listen)
	if err != nil {
		logrus.WithField("err", err).Fatal("Start error")
	}

	grpcServer := grpc.NewServer()
	proto.RegisterSchedulerServer(grpcServer, newServer())
	logrus.Infof("Going to server scheduler at %s", global.Config.GRPC.Listen)
	grpcServer.Serve(lis)
}

func init() {
	flag.Parse()

	if err := global.Init(); err != nil {
		logrus.WithField("err", err).Fatal("Init error")
	}
}
