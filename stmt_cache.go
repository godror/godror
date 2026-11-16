// Copyright 2026 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

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

// newStmtCache returns a cache of at most max statements.
// With max less than 1 nothing is cached.
func newStmtCache(max int) stmtCache {
	if max < 1 {
		return stmtCache{}
	}
	return stmtCache{cache: sieve.NewCache[string, *statement](max)}
}

// take removes and returns the statement cached for query, or nil.
//
// The statement must not stay in the cache while it is in use: closing the
// connection closes the cached statements, and a failed statement is closed
// by its user.
func (sc *stmtCache) take(query string) *statement {
	if sc.cache == nil {
		return nil
	}
	st, _ := sc.cache.Delete(query)
	return st
}

// put adds st to the cache, and closes the statement it evicts or replaces.
func (sc *stmtCache) put(query string, st *statement) {
	if sc.cache == nil {
		st.Close()
		return
	}
	if old, ok := sc.cache.Put(query, st); ok && old != st {
		old.Close()
	}
}

// clear closes and drops all cached statements.
func (sc *stmtCache) Close() error {
	if sc.cache == nil {
		return nil
	}
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

// ErrArgCount is returned by Exec when the number of the arguments differs
// from the number of the placeholders of the statement.
var ErrArgCount = errors.New("wrong number of arguments")

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

	// database/sql checks the number of the arguments only for a prepared
	// statement. It must be checked here, too: the variables of the previous
	// execution are still bound to the statement, so with fewer arguments it
	// would be executed with the previous values of the others, instead of
	// failing with ORA-01008.
	n, names, err := st.BindNames()
	if err == nil && names != nil {
		// n may count a name more than once, as NumInput explains.
		n = len(names)
	}
	if err == nil && n != len(args) {
		err = fmt.Errorf("%w: expected %d, got %d", ErrArgCount, n, len(args))
	}
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	// Keep only DML and PL/SQL: executing the statement handle of a DDL again
	// does nothing, without an error. ODPI-C knows only CREATE, ALTER and DROP
	// as DDL, the others (TRUNCATE, GRANT, COMMENT...) have no type at all.
	reusable := st.dpiStmtInfo.isDML != 0 || st.dpiStmtInfo.isPLSQL != 0

	res, err := st.ExecContext(ctx, args)
	if err != nil || !reusable || st.DeleteFromCache() || st.conn == nil || st.dpiStmt == nil {
		// Do not keep the statement of a failed call either: it may be
		// in an unknown state, and it holds the bind buffers (and the
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

// applyOptions collects the Options among args for the coming execution of
// st, as statement.CheckNamedValue does for a prepared statement, and returns
// the bind arguments without them.
//
// They are the options of that execution only: the statement may be reused by
// the next Exec of the same text, which must not inherit them.
func (st *statement) applyOptions(args []driver.NamedValue) []driver.NamedValue {
	st.argOptions = stmtOptions{}
	n := 0
	for _, arg := range args {
		if opt, ok := arg.Value.(Option); ok {
			if opt != nil {
				opt(&st.argOptions)
			}
			continue
		}
		arg.Ordinal = n + 1
		args[n] = arg
		n++
	}
	return args[:n]
}
