package global

// Init global包初始化
func Init() error {
	if err := initConfig(); err != nil {
		return err
	}
	return nil
}
