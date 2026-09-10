package lifecycle

// Logger is the logging interface lifecycle writes to. *slog.Logger satisfies it.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NopLogger is a Logger that discards everything logged to it. It's used
// whenever a Node is created without a logger.
type NopLogger struct{}

// Info discards the message.
func (NopLogger) Info(string, ...any) {}

// Warn discards the message.
func (NopLogger) Warn(string, ...any) {}

// Error discards the message.
func (NopLogger) Error(string, ...any) {}
