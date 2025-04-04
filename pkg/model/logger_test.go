package model

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestDefaultLogger(t *testing.T) {
	// Setup a buffer to capture log output
	buffer := &bytes.Buffer{}

	// Create a logger that writes to our buffer
	customLogger := log.New(buffer, "", log.LstdFlags)

	// Create a DefaultLogger that uses our custom logger
	logger := &DefaultLogger{
		level:  LogLevelDebug,
		logger: customLogger,
	}

	// Test all log levels with DEBUG level set
	logger.Debug("debug message")
	if !strings.Contains(buffer.String(), "[DEBUG] debug message") {
		t.Error("Expected debug message to be logged")
	}
	buffer.Reset()

	logger.Info("info message")
	if !strings.Contains(buffer.String(), "[INFO] info message") {
		t.Error("Expected info message to be logged")
	}
	buffer.Reset()

	logger.Warn("warn message")
	if !strings.Contains(buffer.String(), "[WARN] warn message") {
		t.Error("Expected warn message to be logged")
	}
	buffer.Reset()

	logger.Error("error message")
	if !strings.Contains(buffer.String(), "[ERROR] error message") {
		t.Error("Expected error message to be logged")
	}
	buffer.Reset()

	// Test log level filtering
	logger.level = LogLevelInfo

	logger.Debug("debug message")
	if strings.Contains(buffer.String(), "[DEBUG] debug message") {
		t.Error("Debug message should not be logged at Info level")
	}
	buffer.Reset()

	logger.Info("info message")
	if !strings.Contains(buffer.String(), "[INFO] info message") {
		t.Error("Expected info message to be logged at Info level")
	}
	buffer.Reset()

	logger.level = LogLevelWarn
	logger.Info("info message")
	if strings.Contains(buffer.String(), "[INFO] info message") {
		t.Error("Info message should not be logged at Warn level")
	}
	buffer.Reset()

	logger.level = LogLevelError
	logger.Warn("warn message")
	if strings.Contains(buffer.String(), "[WARN] warn message") {
		t.Error("Warn message should not be logged at Error level")
	}
	buffer.Reset()
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	// Just ensure these calls don't panic
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	// No assertions needed since NoOpLogger discards all messages
}

func TestSetDefaultLogger(t *testing.T) {
	// Save the original default logger
	original := DefaultLoggerInstance
	defer func() {
		DefaultLoggerInstance = original
	}()

	// Set a custom logger
	customLogger := NewNoOpLogger()
	SetDefaultLogger(customLogger)

	if GetDefaultLogger() != customLogger {
		t.Error("Expected GetDefaultLogger to return the custom logger")
	}
}
