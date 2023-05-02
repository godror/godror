// Copyright 2017, 2021 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"

	"golang.org/x/exp/slog"
)

type logCtxKey struct{}

func getLogger(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if lgr, ok := ctx.Value(logCtxKey{}).(*slog.Logger); ok {
			return lgr
		}
		if ctxValue := ctx.Value(paramsCtxKey{}); ctxValue != nil {
			if cc, ok := ctxValue.(commonAndConnParams); ok {
				return cc.Logger
			}
		}
	}
	return nil
}

// ContextWithLogger returns a context with the given logger.
func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, logCtxKey{}, logger)
}
