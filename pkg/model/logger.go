package model

import (
	"log"
	"os"
)

// Logger defines the interface for logging operations
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	IsLevelEnabled(level LogLevel) bool
}

// LogLevel represents the severity level of a log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// DefaultLogger implements the Logger interface using the standard log package
type DefaultLogger struct {
	level  LogLevel
	logger *log.Logger
}

// NewDefaultLogger creates a new DefaultLogger with the specified log level
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debug logs a debug message
func (l *DefaultLogger) Debug(format string, args ...interface{}) {
	if l.level <= LogLevelDebug {
		l.logger.Printf("[DEBUG] "+format, args...)
	}
}

// Info logs an informational message
func (l *DefaultLogger) Info(format string, args ...interface{}) {
	if l.level <= LogLevelInfo {
		l.logger.Printf("[INFO] "+format, args...)
	}
}

// Warn logs a warning message
func (l *DefaultLogger) Warn(format string, args ...interface{}) {
	if l.level <= LogLevelWarn {
		l.logger.Printf("[WARN] "+format, args...)
	}
}

// Error logs an error message
func (l *DefaultLogger) Error(format string, args ...interface{}) {
	if l.level <= LogLevelError {
		l.logger.Printf("[ERROR] "+format, args...)
	}
}

// IsLevelEnabled returns true if the given log level is enabled
func (l *DefaultLogger) IsLevelEnabled(level LogLevel) bool {
	return l.level <= level
}

// NoOpLogger is a logger implementation that discards all log messages
type NoOpLogger struct{}

// NewNoOpLogger creates a new NoOpLogger
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

// Debug discards the debug message
func (l *NoOpLogger) Debug(format string, args ...interface{}) {}

// Info discards the informational message
func (l *NoOpLogger) Info(format string, args ...interface{}) {}

// Warn discards the warning message
func (l *NoOpLogger) Warn(format string, args ...interface{}) {}

// Error discards the error message
func (l *NoOpLogger) Error(format string, args ...interface{}) {}

// IsLevelEnabled always returns false for NoOpLogger
func (l *NoOpLogger) IsLevelEnabled(level LogLevel) bool {
	return false
}

var (
	// DefaultLoggerInstance is the default logger used by the package
	DefaultLoggerInstance Logger = NewDefaultLogger(LogLevelInfo)
)

// SetDefaultLogger sets the default logger instance
func SetDefaultLogger(logger Logger) {
	DefaultLoggerInstance = logger
}

// GetDefaultLogger returns the current default logger instance
func GetDefaultLogger() Logger {
	return DefaultLoggerInstance
}
