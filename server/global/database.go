package global

import "github.com/go-pg/pg"

// DefaultDB 默认的数据库
var DefaultDB *pg.DB

// GetDBByTaskGroup 将来可能会有按照taskGroup进行分库的需求现在只返回默认的DB
func GetDBByTaskGroup(taskGroup string) *pg.DB {
	return DefaultDB.WithParam("schema", pg.F(taskGroup))
}
