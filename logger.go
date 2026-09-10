package lifecycle

type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NopLogger is a Logger that discards everything logged to it. It's used
// whenever a Node is created without a logger.
type NopLogger struct{}

func (NopLogger) Info(msg string, args ...any)  {}
func (NopLogger) Warn(msg string, args ...any)  {}
func (NopLogger) Error(msg string, args ...any) {}
