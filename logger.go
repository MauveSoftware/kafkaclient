package kafkaclient

// Logger writes messages to log
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

var logger Logger

func init() {
	logger = &nopLogger{}
}

// SetLogger sets the logger used by kafkaclient library
func SetLogger(l Logger) {
	logger = l
}

type nopLogger struct {
}

// Infof implements Logger.Infof
func (nopLogger) Infof(format string, args ...interface{}) {
}

// Warnf implements Logger.Warnf
func (nopLogger) Warnf(format string, args ...interface{}) {
}
