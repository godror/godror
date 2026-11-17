# Execution and resource lifetimes

Plain Exec uses database/sql's prepare, execute, close path. There is no
implicit Go statement cache; stmtCacheSize controls only the Oracle Client
cache. Use PrepareContext and close the resulting statement explicitly when
repeated calls should reuse bind variables. This is especially useful for
object collections, where repeated allocation can grow Oracle Client memory.
Removing the implicit cache does not fix that client allocation issue.

Object reuse remains limited to explicitly reused statements. The ownership
and stale-value checks remain necessary there: an ObjectWriter must see an
empty object and must never overwrite a caller-owned Object. A future simpler
implementation should separate reusable internal bind buffers from public
Object values, with explicit reset/overwrite operations rather than lazy
reset on reads. Removing those checks alone is not a safe simplification.

## Column scanning

RowsColumnScanner is not supported yet, including on Go 1.27. database/sql
continues to use driver.Rows.Next and its normal conversion path. The direct
scanner duplicated NULL, numeric, timestamp and destination-pointer handling,
and required fixes to maintain parity. Reintroducing it should require a
shared conversion implementation, parity tests and representative benchmark
results demonstrating that the maintenance cost is justified. Ordinary Scan
continues to be supported.

## LOBs

A LOB column returned with LobAsReader is valid until the next rows.Next or
rows.Close, even when the statement was explicitly prepared. Read it inside
the iteration before advancing. QueryRow closes its rows before Scan returns,
so use QueryContext for streaming LOB columns. Keeping the statement open does
not extend a row LOB's lifetime. OUT LOBs remain valid until their statement is
executed again or closed. LOBs read from an Object remain tied to that Object.

## Nested cursors

Advancing the parent rows does not automatically close a previously returned
nested cursor. Close every child cursor explicitly when finished; retaining
many open children can exhaust Oracle's OPEN_CURSORS limit. Keep the parent
rows and connection open while consuming children; database/sql can close
child sql.Rows when its parent closes. For a driver.Rows obtained through
QueryRow, explicitly close that driver.Rows after consuming it. There is no
special ErrCursorClosed error on advancing the parent.
