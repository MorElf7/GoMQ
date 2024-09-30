package utils

import (
	"io"
	"log"
	"os"
)

type LoggerType struct {
	InfoLogger  *log.Logger
	ErrorLogger *log.Logger
}

func NewLogger(filePath string) *LoggerType {
	// Open or create the log file with write permissions
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer file.Close()

	// Create a multi writer that writes to both stdout and the log file
	mw := io.MultiWriter(os.Stdout, file)

	return &LoggerType{
		InfoLogger:  log.New(mw, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
		ErrorLogger: log.New(mw, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (l *LoggerType) Info(format string, a ...any) {
	l.InfoLogger.Printf(format, a...)
}
func (l *LoggerType) Error(format string, a ...any) {
	l.ErrorLogger.Printf(format, a...)
}
