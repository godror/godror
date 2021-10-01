// Copyright 2017, 2021 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"io"
	"sync"

	"github.com/go-logfmt/logfmt"
)

var globalLogger = &swapLogger{}

func SetLogger(logger Logger) {
	globalLogger.Swap(logger)
}
func SetLog(f func(...interface{}) error) {
	globalLogger.Swap(LogFunc(f))
}

// NewLogfmtLogger returns a Logger, and that logs using logfmt, to the given io.Writer.
func NewLogfmtLogger(w io.Writer) Logger {
	enc := logfmt.NewEncoder(w)
	return LogFunc(func(keyvals ...interface{}) error {
		firstErr := enc.EncodeKeyvals(keyvals...)
		if err := enc.EndRecord(); err != nil && firstErr == nil {
			return err
		}
		return firstErr
	})
}

type Logger interface {
	Log(keyvals ...interface{}) error
}
type LogFunc func(keyvals ...interface{}) error

func (f LogFunc) Log(keyvals ...interface{}) error { return f(keyvals...) }

type swapLogger struct {
	mu     sync.RWMutex
	logger Logger
}

func (sw *swapLogger) IsSet() bool {
	sw.mu.RLock()
	isset := sw.logger != nil
	sw.mu.RUnlock()
	return isset
}

func (sw *swapLogger) Swap(logger Logger) {
	sw.mu.Lock()
	sw.logger = logger
	sw.mu.Unlock()
}
func (sw *swapLogger) Log(keyvals ...interface{}) error {
	sw.mu.RLock()
	logger := sw.logger
	sw.mu.RUnlock()
	if logger == nil {
		return nil
	}
	return logger.Log(keyvals...)
}

type logCtxKey struct{}

func getLogger() Logger {
	if globalLogger.IsSet() {
		return globalLogger
	}
	return nil
}
func ctxGetLog(ctx context.Context) Logger {
	if ctx != nil {
		if lgr, ok := ctx.Value(logCtxKey{}).(Logger); ok {
			return lgr
		}
	}
	return getLogger()
}

// ContextWithLogger returns a context with the given logger.
func ContextWithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, logCtxKey{}, logger)
}

// ContextWithLog returns a context with the given log function.
func ContextWithLog(ctx context.Context, logF func(...interface{}) error) context.Context {
	return context.WithValue(ctx, logCtxKey{}, LogFunc(logF))
}
