// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/godror/godror/internal/sieve"
)

// defaultStmtCacheSize is the number of prepared statements kept per
// connection for ExecContext, if StmtCacheSize is not set in the
// connection parameters.
const defaultStmtCacheSize = 20

// stmtCache is a per-connection SIEVE cache of the statements prepared for
// ExecContext.
//
// database/sql prepares a new statement (and closes it) for every Exec, unless
// the driver implements driver.ExecerContext. Preparing a statement with an
// OUT object collection bind makes the Oracle Client allocate the client-side
// memory of the collection's elements, which it does not free when the
// statement is closed: keep the statement (and its bind variables) and
// re-execute it instead. See TestPlSqlNestedObj.
type stmtCache struct {
	cache *sieve.Cache[string, *statement]
}

func newStmtCache(max int) stmtCache {
	return stmtCache{cache: sieve.NewCache[string, *statement](max)}
}

// take removes and returns the statement cached for query, or nil.
func (sc *stmtCache) take(query string) *statement {
	st, _ := sc.cache.Get(query)
	return st
}

// put adds st to the cache, and closes the statements it evicts.
func (sc *stmtCache) put(query string, st *statement) {
	sc.cache.Put(query, st)
}

// clear closes and drops all cached statements.
func (sc *stmtCache) Close() error {
	sc.cache.Clear(func(k string, v *statement) {
		v.Close()
	})
	return nil
}

// stmtCacheMax converts the StmtCacheSize connection parameter to the maximum
// number of statements kept in the cache: 0 means the default, negative
// disables the cache.
func stmtCacheMax(stmtCacheSize int) int {
	switch {
	case stmtCacheSize < 0:
		return 0
	case stmtCacheSize == 0:
		return defaultStmtCacheSize
	default:
		return stmtCacheSize
	}
}

var _ driver.ExecerContext = (*conn)(nil)

// ExecContext executes a query that does not return rows.
//
// database/sql prepares a new statement for every Exec when the driver has no
// ExecerContext. That is not only slow, but leaks the client-side memory the
// Oracle Client allocates for an OUT object collection bind (see
// TestPlSqlNestedObj), so reuse the statement cached for the query, if there is
// one.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query == getConnection {
		st, err := c.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer st.Close()
		return st.(driver.StmtExecContext).ExecContext(ctx, args)
	}

	if len(query) > 6 && strings.EqualFold(query[:6], "create") {
		sts, err := c.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		st := sts.(*statement)
		defer st.Close()
		return st.ExecContext(ctx, st.applyOptions(args))
	}
	st := c.stmtCache.take(query)
	if st == nil {
		sts, err := c.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		st = sts.(*statement)
	}

	// The Options are still in args: database/sql gives them to the
	// connection's CheckNamedValue on the ExecerContext path, instead of the
	// statement's one, which strips them for prepared statements.
	args = st.applyOptions(args)

	res, err := st.ExecContext(ctx, args)
	if err != nil || st.DeleteFromCache() || st.conn == nil || st.dpiStmt == nil {
		// Do not keep the statement of a failed call: it may be in an
		// unknown state, and it holds the bind buffers (and the
		// caller's OUT destinations).
		_ = st.Close()
		return res, err
	}
	// Do not retain the caller's OUT destinations and context.
	st.dests = st.dests[:0]
	st.ctx = nil
	c.stmtCache.put(query, st)
	return res, nil
}

// applyOptions applies the Options among args to st, and returns the bind
// arguments without them.
//
// The statement of an Exec may be reused by the next Exec, so start from the
// default options: a plain Exec on a fresh statement sees the same.
func (st *statement) applyOptions(args []driver.NamedValue) []driver.NamedValue {
	st.stmtOptions = stmtOptions{}
	n := 0
	for _, arg := range args {
		if opt, ok := arg.Value.(Option); ok {
			if opt != nil {
				opt(&st.stmtOptions)
			}
			continue
		}
		arg.Ordinal = n + 1
		args[n] = arg
		n++
	}
	return args[:n]
}
