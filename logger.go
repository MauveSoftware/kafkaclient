package kafkaclient

// Logger writes messages to log
type Logger interface {
	// Infof writes info
	Infof(format string, args ...interface{})

	// Errorf writes error
	Errorf(format string, args ...interface{})

	// Warnf writes warning
	Warnf(format string, args ...interface{})

	// Debugf writes debug
	Debugf(format string, args ...interface{})
}

var logger Logger

func StandardLogger() Logger {
	return logger
}

func init() {
	logger = &noopLogger{}
}

// SetLogger sets the logger used by kafkaclient library
func SetLogger(l Logger) {
	logger = l
}

type noopLogger struct {
}

// Infof implements Logger.Infof
func (noopLogger) Infof(format string, args ...interface{}) {
}

// Warnf implements Logger.Warnf
func (noopLogger) Warnf(format string, args ...interface{}) {
}

// Warnf implements Logger.Errorf
func (noopLogger) Errorf(format string, args ...interface{}) {
}

// Debugf implements Logger.Errorf
func (noopLogger) Debugf(format string, args ...interface{}) {
}
