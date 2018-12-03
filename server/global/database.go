package global

import "github.com/go-pg/pg"

// DefaultDB 默认的数据库
var DefaultDB *pg.DB

// GetDBByTaskTopic 将来可能会有按照topic进行分库的需求现在只返回默认的DB
func GetDBByTaskTopic(topic string) *pg.DB {
	return DefaultDB.WithParam("schema", pg.F(topic))
}
