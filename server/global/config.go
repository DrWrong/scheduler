package global

import (
	"encoding/json"
	"flag"
	"os"
	"time"
)

var configFile = flag.String("conf", "", "config path")

// Config 全局配置文件
var Config *config

type config struct {
	GRPC struct {
		Listen string
	}
	TaskFetcher struct {
		DBFetchTaskNumber        int
		CacheMaxDuration         time.Duration
		EmptyMinDuration         time.Duration
		DeliverTaskReplyDuration time.Duration
	}
}

func initConfig() error {
	f, err := os.Open(*configFile)
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(f)

	c := new(config)

	if err := decoder.Decode(c); err != nil {
		return err
	}

	Config = c

	return nil
}
