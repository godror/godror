// Copyright 2017, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

/*
#include "dpiImpl.h"
*/
import "C"
import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"
)

// Lob is for reading/writing a LOB.
//
// A Lob returned by the driver (with LobAsReader, as an OUT parameter or by
// Data.GetLob) holds its own reference to the LOB, so it stays readable after
// the rows are closed or have fetched further, until its connection is closed.
// The reference is released when the Lob is read to EOF, when its Reader is
// closed, or when the connection is closed; reading it after that returns an error.
type Lob struct {
	io.Reader
	IsClob bool
}

var _ = (io.Reader)((*Lob)(nil))
var _ = (io.ReaderAt)((*Lob)(nil))

// Hijack the underlying lob reader/writer, and
// return a DirectLob for reading/writing the lob directly.
//
// After this, the Lob is unusable!
// The DirectLob takes over the Lob's reference, which its Close
// (or closing the connection) releases.
func (lob *Lob) Hijack() (*DirectLob, error) {
	if lob == nil || lob.Reader == nil {
		return nil, errors.New("lob is nil")
	}
	lr, ok := lob.Reader.(*dpiLobReader)
	if !ok {
		return nil, fmt.Errorf("Lob.Reader is %T, not *dpiLobReader", lob.Reader)
	}
	lob.Reader = nil
	// The reader's reference moves to the DirectLob.
	lr.mu.Lock()
	dl := &DirectLob{drv: lr.drv, dpiLob: lr.dpiLob, owned: lr.dpiLob != nil}
	lr.dpiLob = nil
	lr.mu.Unlock()
	if dl.dpiLob != nil {
		untrackLob(lr, dl.dpiLob)
		trackLob(dl, dl.dpiLob)
	}
	return dl, nil
}

// lobHolder is a Go value that holds its own reference to a dpiLob.
type lobHolder interface {
	// dropLob releases the reference when the LOB's connection is closed.
	dropLob()
}

// connLobs tracks the LOB references held by Go values, by connection.
//
// Every dpiLob holds a reference to its dpiConn, so a LOB that is never
// read to EOF or closed would keep a pooled session from being returned
// to the pool. conn.closeNotLocking calls releaseConnLobs to drop them.
var connLobs struct {
	m map[*C.dpiConn]map[lobHolder]struct{}
	sync.Mutex
}

// trackLob records that h holds a reference to lob.
func trackLob(h lobHolder, lob *C.dpiLob) {
	if lob == nil || lob.conn == nil {
		return
	}
	connLobs.Lock()
	if connLobs.m == nil {
		connLobs.m = make(map[*C.dpiConn]map[lobHolder]struct{})
	}
	s := connLobs.m[lob.conn]
	if s == nil {
		s = make(map[lobHolder]struct{})
		connLobs.m[lob.conn] = s
	}
	s[h] = struct{}{}
	connLobs.Unlock()
}

// untrackLob forgets h, which is about to release its reference to lob.
func untrackLob(h lobHolder, lob *C.dpiLob) {
	if lob == nil || lob.conn == nil {
		return
	}
	connLobs.Lock()
	if s := connLobs.m[lob.conn]; s != nil {
		delete(s, h)
		if len(s) == 0 {
			delete(connLobs.m, lob.conn)
		}
	}
	connLobs.Unlock()
}

// releaseConnLobs releases the LOB references still held on dpiConn.
func releaseConnLobs(dpiConn *C.dpiConn) {
	connLobs.Lock()
	s := connLobs.m[dpiConn]
	delete(connLobs.m, dpiConn)
	connLobs.Unlock()
	for h := range s {
		h.dropLob()
	}
}

// errLobConnClosed is returned when reading a LOB whose connection is closed.
var errLobConnClosed = errors.New("the LOB's connection is closed")

// dataLob is the LOB reference that ODPI-C hands over with an object
// attribute or collection element. The Data that holds it releases it.
type dataLob struct {
	lob *C.dpiLob
	mu  sync.Mutex
}

func newDataLob(lob *C.dpiLob) *dataLob {
	dl := &dataLob{lob: lob}
	trackLob(dl, lob)
	return dl
}

// get returns the LOB, or nil if its connection has been closed.
func (dl *dataLob) get() *C.dpiLob {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.lob
}

func (dl *dataLob) release() {
	dl.mu.Lock()
	lob := dl.lob
	dl.lob = nil
	dl.mu.Unlock()
	if lob != nil {
		untrackLob(dl, lob)
		C.dpiLob_release(lob)
	}
}

func (dl *dataLob) dropLob() {
	dl.mu.Lock()
	if dl.lob != nil {
		C.dpiLob_release(dl.lob)
		dl.lob = nil
	}
	dl.mu.Unlock()
}

// WriteTo writes data to w until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written. Any error encountered during the write is also returned.
//
// Will use Lob.Reader.WriteTo, if Lob.Reader implements io.WriterTo.
func (lob *Lob) WriteTo(w io.Writer) (n int64, err error) {
	if wt, ok := lob.Reader.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.CopyBuffer(w, lob.Reader, make([]byte, 1<<20))
}

// NewBufferedReader returns a new bufio.Reader with the given size (or 1M if 0).
func (lob *Lob) NewBufferedReader(size int) *bufio.Reader {
	if size <= 0 {
		size = 1 << 20
	}
	return bufio.NewReaderSize(lob.Reader, size)
}

// Size exposes the underlying Reader's Size method, if it is supported.
func (lob *Lob) Size() (int64, error) {
	if lr, ok := lob.Reader.(interface{ Size() (int64, error) }); ok {
		return lr.Size()
	}
	return 0, ErrNotSupported
}

// ReadAt exposes the underlying Reader's ReadAt method, if it is supported.
func (lob *Lob) ReadAt(p []byte, off int64) (int, error) {
	if lr, ok := lob.Reader.(io.ReaderAt); ok {
		return lr.ReadAt(p, off)
	}
	return 0, ErrNotSupported
}

// Scan assigns a value from a database driver.
//
// The src value will be of one of the following types:
//
//	int64
//	float64
//	bool
//	[]byte
//	string
//	time.Time
//	nil - for NULL values
//
// An error should be returned if the value cannot be stored
// without loss of information.
func (dlr *dpiLobReader) Scan(src any) error {
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot convert LOB to %T", src)
	}
	_ = b
	return nil
}

var _ = io.ReadCloser((*dpiLobReader)(nil))
var _ = io.ReaderAt((*dpiLobReader)(nil))

type dpiLobReader struct {
	*drv
	dpiLob              *C.dpiLob
	buf                 []byte
	offset, sizePlusOne C.uint64_t
	mu                  sync.Mutex
	chunkSize           C.uint32_t
	bufR, bufW          int
	finished            bool
	IsClob              bool
	// connClosed is set when the connection was closed before the reader
	// released its reference.
	connClosed bool
}

// newLobReader returns a reader that owns the given reference to lob.
func newLobReader(d *drv, lob *C.dpiLob, isClob bool) *dpiLobReader {
	dlr := &dpiLobReader{drv: d, dpiLob: lob, IsClob: isClob}
	trackLob(dlr, lob)
	return dlr
}

func (dlr *dpiLobReader) dropLob() {
	dlr.mu.Lock()
	if dlr.dpiLob != nil {
		C.dpiLob_release(dlr.dpiLob)
		dlr.dpiLob = nil
		dlr.connClosed = true
	}
	dlr.mu.Unlock()
}

// WriteTo writes data to w until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written. Any error encountered during the write is also returned.
//
// Uses efficient, multiple-of-LOB-chunk-size buffered reads.
func (dlr *dpiLobReader) WriteTo(w io.Writer) (n int64, err error) {
	size := dlr.ChunkSize()
	const minBufferSize = 1 << 20
	if size <= 0 {
		size = minBufferSize
	} else {
		for size < minBufferSize/2 { // at most 1M
			size *= 2
		}
	}
	return io.CopyBuffer(
		w,
		// Mask WriteTo method
		io.Reader(struct {
			io.Reader
		}{dlr}),
		make([]byte, size))
}

// ChunkSize returns the LOB's native chunk size. Reads/writes with a multiply of this size is the most performant.
func (dlr *dpiLobReader) ChunkSize() int {
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	if dlr.chunkSize != 0 {
		return int(dlr.chunkSize)
	}
	if err := dlr.checkExec(func() C.int {
		return C.dpiLob_getChunkSize(dlr.dpiLob, &dlr.chunkSize)
	}); err != nil {
		dlr.chunkSize = 0
		return -1
	}
	return int(dlr.chunkSize)
}

// Read from LOB. It does buffer the reading internally against short buffers (io.ReadAll).
func (dlr *dpiLobReader) Read(p []byte) (int, error) {
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	if dlr.connClosed {
		return 0, errLobConnClosed
	}
	logger := getLogger(context.TODO())
	if logger != nil {
		logger.Debug("Read", "bufR", dlr.bufR, "bufW", dlr.bufW, "buf", cap(dlr.buf))
	}

	if dlr.buf == nil {
		if dlr.chunkSize == 0 {
			if err := dlr.checkExec(func() C.int {
				return C.dpiLob_getChunkSize(dlr.dpiLob, &dlr.chunkSize)
			}); err != nil {
				return 0, fmt.Errorf("getChunkSize: %w", err)
			}
		}
		// If the dest buffer is big enough, avoid copying.
		if ulen := C.uint64_t(len(p)); ulen >= C.uint64_t(dlr.chunkSize) || dlr.sizePlusOne != 0 && ulen+1 >= dlr.sizePlusOne {
			if logger != nil {
				logger.Debug("direct read", "p", len(p), "chunkSize", dlr.chunkSize)
			}
			return dlr.read(p)
		}
		cs := int(dlr.chunkSize)
		dlr.buf = make([]byte, (maxI(len(p), 1<<20-cs)/cs+1)*cs)
		dlr.bufR, dlr.bufW = 0, 0
	} else if dlr.bufW != 0 && cap(dlr.buf) != 0 {
		var n int
		if dlr.bufR < dlr.bufW {
			n = copy(p, dlr.buf[dlr.bufR:dlr.bufW])
			dlr.bufR += n
		}
		if dlr.bufR == dlr.bufW {
			dlr.bufR, dlr.bufW = 0, 0
		}
		if n != 0 {
			return n, nil
		}
	}
	var err error
	// We only read into dlr.buf when it's empty, dlr.bufR == dlr.bufW == 0
	// nosemgrep: trailofbits.go.questionable-assignment.questionable-assignment
	dlr.bufW, err = dlr.read(dlr.buf)
	if logger != nil {
		logger.Debug("dlr.read", "bufR", dlr.bufR, "bufW", dlr.bufW, "chunkSize", dlr.chunkSize, "error", err)
	}
	dlr.bufR = copy(p, dlr.buf[:dlr.bufW])
	if err == io.EOF && dlr.bufW != dlr.bufR {
		err = nil
	}
	return dlr.bufR, err
}

var ErrCLOB = errors.New("CLOB is not supported")

// Size returns the LOB's size. It returns ErrCLOB for CLOB,
// (works only for BLOBs), as Oracle reports CLOB size in runes, not in bytes!
func (dlr *dpiLobReader) Size() (int64, error) {
	dlr.mu.Lock()
	err := dlr.getSize()
	size := dlr.sizePlusOne - 1
	isClob := dlr.IsClob
	dlr.mu.Unlock()
	if err == nil && isClob {
		err = ErrCLOB
	}
	return int64(size), err
}
func (dlr *dpiLobReader) getSize() error {
	if dlr.connClosed {
		return errLobConnClosed
	}
	if dlr.sizePlusOne != 0 {
		return nil
	}
	var err error
	runtime.LockOSThread()
	if err = dlr.checkExecNoLOT(func() C.int {
		return C.dpiLob_getSize(dlr.dpiLob, &dlr.sizePlusOne)
	}); err != nil {
		err = fmt.Errorf("getSize: %w", err)
		dlr.closeLob()
	}
	runtime.UnlockOSThread()
	dlr.sizePlusOne++
	return err
}

// read does the real LOB reading.
func (dlr *dpiLobReader) read(p []byte) (int, error) {
	if dlr == nil {
		return 0, errors.New("read on nil dpiLobReader")
	}
	logger := getLogger(context.TODO())
	if logger != nil {
		logger.Debug("LOB Read", "dlr", fmt.Sprintf("%p", dlr.dpiLob), "offset", dlr.offset, "size", dlr.sizePlusOne, "finished", dlr.finished, "clob", dlr.IsClob)
	}
	if dlr.finished || dlr.dpiLob == nil {
		return 0, io.EOF
	}
	if len(p) < 1 || dlr.IsClob && len(p) < 4 {
		return 0, io.ErrShortBuffer
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	// For CLOB, sizePlusOne and offset counts the CHARACTERS!
	// See https://oracle.github.io/odpi/doc/public_functions/dpiLob.html dpiLob_readBytes
	var lobType C.dpiOracleTypeNum
	if dlr.sizePlusOne == 0 { // first read
		// never read size before
		if err := dlr.getSize(); err != nil {
			var coder interface{ Code() int }
			if errors.As(err, &coder) && coder.Code() == 22922 || strings.Contains(err.Error(), "invalid dpiLob handle") {
				return 0, io.EOF
			}
			return 0, err
		}

		dlr.IsClob = false
		if err := dlr.checkExecNoLOT(func() C.int {
			return C.dpiLob_getType(dlr.dpiLob, &lobType)
		}); err != nil {
			return 0, err
		} else if lobType == C.DPI_ORACLE_TYPE_CLOB || lobType == C.DPI_ORACLE_TYPE_NCLOB {
			dlr.IsClob = true
		}
	}
	n := C.uint64_t(len(p))
	amount := n
	if dlr.IsClob {
		amount /= 4 // dpiLob_readBytes' amount is the number of CHARACTERS for CLOBs.
	}
	if logger != nil {
		logger.Debug("Read", "offset", dlr.offset, "sizePlusOne", dlr.sizePlusOne, "n", n, "amount", amount, "lobType", lobType)
	}
	if !dlr.IsClob && dlr.offset+1 >= dlr.sizePlusOne {
		if logger != nil {
			logger.Debug("LOB reached end", "offset", dlr.offset, "size", dlr.sizePlusOne)
		}
		return 0, io.EOF
	}
	rd := func() error {
		return dlr.drv.checkExecNoLOT(func() C.int {
			return C.dpiLob_readBytes(dlr.dpiLob, dlr.offset+1, amount, (*C.char)(unsafe.Pointer(&p[0])), &n)
		})
	}
	if err := rd(); err != nil {
		var codeErr interface{ Code() int }
		errors.As(err, &codeErr)
		if logger != nil {
			logger.Warn("readBytes", "offset", dlr.offset, "amount", amount, "error", err)
		}
		if dlr.IsClob && codeErr != nil && codeErr.Code() == 22831 {
			amount--
			if err = rd(); err != nil {
				logger.Warn("readBytes", "offset", dlr.offset, "amount", amount, "error", err)
				dlr.offset--
				err = rd()
			}
		}
		if err != nil {
			dlr.closeLob()
			if dlr.finished = codeErr != nil && codeErr.Code() == 1403; dlr.finished {
				dlr.offset += n
				return int(n), io.EOF
			}
			return int(n), fmt.Errorf("dpiLob_readbytes(lob=%p offset=%d n=%d amount=%d): %w", dlr.dpiLob, dlr.offset, len(p), amount, err)
		}
	}
	if logger != nil {
		logger.Debug("read", "n", n, "amount", amount, "offset", dlr.offset)
	}
	if !dlr.IsClob {
		dlr.offset += n
	} else {
		// trim last erroneous encodings
		for {
			r, size := utf8.DecodeLastRune(p[:n])
			if size == 0 || r != utf8.RuneError {
				break
			}
			if logger != nil {
				logger.Warn("LastRune", "r", r, "size", size, "n", n)
			}
			n--
		}
		if !dlr.IsClob {
			dlr.offset += C.uint64_t(utf8.RuneCount(p[:n]))
		} else {
			// NCLOB must be count in UTF-16
			for i, n := 0, int(n); i < n; {
				r, size := utf8.DecodeRune(p[i:n])
				i += size
				dlr.offset += C.uint64_t(utf16.RuneLen(r))
			}
		}
	}
	var err error
	if amount != 0 && n == 0 || !dlr.IsClob && dlr.offset+1 >= dlr.sizePlusOne {
		dlr.closeLob()
		dlr.finished = true
		err = io.EOF
	}
	if logger != nil {
		logger.Debug("LOB", "n", n, "offset", dlr.offset, "size", dlr.sizePlusOne, "finished", dlr.finished, "clob", dlr.IsClob, "error", err)
	}
	return int(n), err
}

// ReadAt reads at the specified offset (in bytes).
// Works only for BLOBs!
func (dlr *dpiLobReader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		panic("ReadAt into a zero-length buffer")
	}
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	if dlr.connClosed {
		return 0, errLobConnClosed
	}
	if dlr.IsClob {
		return 0, ErrCLOB
	}
	n := C.uint64_t(len(p))
	err := dlr.checkExec(func() C.int {
		return C.dpiLob_readBytes(dlr.dpiLob, C.uint64_t(off+1), n, (*C.char)(unsafe.Pointer(&p[0])), &n)
	})
	if err != nil {
		err = fmt.Errorf("readBytes at %d for %d: %w", off, n, dlr.getError())
	}
	return int(n), err
}

// closeLob closes the LOB early (at EOF or on error) and drops the reader's
// reference to it. dlr.mu must be held.
func (dlr *dpiLobReader) closeLob() {
	if dlr.dpiLob == nil {
		return
	}
	untrackLob(dlr, dlr.dpiLob)
	C.dpiLob_close(dlr.dpiLob)
	C.dpiLob_release(dlr.dpiLob)
	dlr.dpiLob = nil
}

// Close releases the reader's reference to the LOB.
func (dlr *dpiLobReader) Close() error {
	if dlr == nil {
		return nil
	}
	dlr.mu.Lock()
	lob := dlr.dpiLob
	dlr.dpiLob = nil
	dlr.mu.Unlock()
	if lob == nil {
		return nil
	}
	untrackLob(dlr, lob)
	return closeLob(dlr, lob)
}

type dpiLobWriter struct {
	*drv
	dpiLob  *C.dpiLob
	offset  C.uint64_t
	opened  bool
	isClob  bool
	tail    [utf8.UTFMax - 1]byte // CLOB: incomplete UTF-8 sequence at the end of the last Write
	tailLen int
	buf     []byte
}

// Write writes p at the end of the LOB.
//
// For CLOBs, the offset is in characters (UTF-16 code units, as Oracle stores
// CLOBs in UTF-16), so only whole UTF-8 sequences are written,
// and an incomplete one at the end of p is kept for the next Write (or flush).
func (dlw *dpiLobWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !dlw.isClob {
		return dlw.write(p, C.uint64_t(len(p)))
	}
	buf := p
	if dlw.tailLen != 0 {
		buf = append(append(dlw.buf[:0], dlw.tail[:dlw.tailLen]...), p...)
		dlw.buf = buf
	}
	// Find the start of the last, incomplete UTF-8 sequence.
	end := len(buf)
	for i := len(buf) - 1; i >= 0 && i >= len(buf)-(utf8.UTFMax-1); i-- {
		if utf8.RuneStart(buf[i]) {
			if !utf8.FullRune(buf[i:]) {
				end = i
			}
			break
		}
	}
	var chars C.uint64_t
	for i := 0; i < end; {
		r, size := utf8.DecodeRune(buf[i:end])
		i += size
		chars += C.uint64_t(utf16.RuneLen(r))
	}
	if end != 0 {
		if _, err := dlw.write(buf[:end], chars); err != nil {
			return 0, err
		}
	}
	dlw.tailLen = copy(dlw.tail[:], buf[end:])
	return len(p), nil
}

// flush writes the remaining incomplete UTF-8 sequence of a CLOB, if any.
func (dlw *dpiLobWriter) flush() error {
	if dlw.tailLen == 0 {
		return nil
	}
	n := dlw.tailLen
	dlw.tailLen = 0
	_, err := dlw.write(dlw.tail[:n], C.uint64_t(n))
	return err
}

// write p at the offset, then advance the offset by length.
func (dlw *dpiLobWriter) write(p []byte, length C.uint64_t) (int, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	lob := dlw.dpiLob
	if !dlw.opened {
		if err := dlw.drv.checkExecNoLOT(func() C.int {
			return C.dpiLob_openResource(lob)
		}); err != nil {
			return 0, fmt.Errorf("openResources(%p): %w", lob, err)
		}
		dlw.opened = true
	}

	n := C.uint64_t(len(p))
	if err := dlw.drv.checkExecNoLOT(func() C.int {
		return C.dpiLob_writeBytes(lob, dlw.offset+1, (*C.char)(unsafe.Pointer(&p[0])), n)
	}); err != nil {
		err = fmt.Errorf("writeBytes(%p, offset=%d, data=%d): %w", lob, dlw.offset, n, err)
		dlw.dpiLob = nil
		_ = closeLob(dlw, lob)
		return 0, err
	}
	dlw.offset += length

	return int(n), nil
}

func (dlw *dpiLobWriter) Close() error {
	if dlw == nil || dlw.dpiLob == nil {
		return nil
	}
	lob := dlw.dpiLob
	dlw.dpiLob = nil
	return closeLob(dlw, lob)
}

func closeLob(d interface{ getError() error }, lob *C.dpiLob) error {
	if lob == nil {
		return nil
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var isOpen C.int
	if C.dpiLob_getIsResourceOpen(lob, &isOpen) != C.DPI_FAILURE && isOpen == 1 {
		if C.dpiLob_closeResource(lob) == C.DPI_FAILURE {
			if err := d.getError(); err != nil {
				var codeErr interface{ Code() int }
				if errors.As(err, &codeErr) && codeErr.Code() != 22289 { // cannot perform %s operation on an unopened file or LOB
					return fmt.Errorf("closeResource(%p): %w", lob, err)
				}
			}
		}
	}
	C.dpiLob_release(lob)
	return nil
}

// DirectLob holds a Lob and allows direct (Read/WriteAt, not streaming Read/Write) operations on it.
type DirectLob struct {
	drv            *drv
	dpiLob         *C.dpiLob
	opened, isClob bool
	// owned is true when dpiLob was created by us (NewTempLob),
	// so Close must release it even if it was never opened.
	owned bool
	mu    sync.Mutex
}

func (dl *DirectLob) dropLob() {
	dl.mu.Lock()
	if dl.dpiLob != nil {
		C.dpiLob_release(dl.dpiLob)
		dl.opened, dl.owned, dl.dpiLob = false, false, nil
	}
	dl.mu.Unlock()
}

var _ = io.ReaderAt((*DirectLob)(nil))
var _ = io.WriterAt((*DirectLob)(nil))

// NewTempLob returns a temporary LOB as DirectLob.
func (c *conn) NewTempLob(isClob bool) (*DirectLob, error) {
	typ := C.uint(C.DPI_ORACLE_TYPE_BLOB)
	if isClob {
		typ = C.DPI_ORACLE_TYPE_CLOB
	}
	lob := DirectLob{drv: c.drv, isClob: isClob, owned: true}
	if err := c.checkExec(func() C.int { return C.dpiConn_newTempLob(c.dpiConn, typ, &lob.dpiLob) }); err != nil {
		return nil, fmt.Errorf("newTempLob: %w", err)
	}
	trackLob(&lob, lob.dpiLob)
	return &lob, nil
}

// Close the Lob.
func (dl *DirectLob) Close() error {
	if dl == nil {
		return nil
	}
	dl.mu.Lock()
	if dl.dpiLob == nil || !(dl.opened || dl.owned) {
		dl.mu.Unlock()
		return nil
	}
	lob := dl.dpiLob
	dl.opened, dl.owned, dl.dpiLob = false, false, nil
	dl.mu.Unlock()
	untrackLob(dl, lob)
	return closeLob(dl.drv, lob)
}

// Size returns the size of the LOB.
//
// WARNING: for historical reasons, Oracle stores CLOBs and NCLOBs using the UTF-16 encoding,
// regardless of what encoding is otherwise in use by the database.
// The number of characters, however, is defined by the number of UCS-2 codepoints.
// For this reason, if a character requires more than one UCS-2 codepoint,
// the size returned will be inaccurate and care must be taken to account for the difference!
func (dl *DirectLob) Size() (int64, error) {
	var n C.uint64_t
	if dl.dpiLob == nil {
		return 0, nil
	}
	if err := dl.drv.checkExec(func() C.int {
		return C.dpiLob_getSize(dl.dpiLob, &n)
	}); err != nil {
		var coder interface{ Code() int }
		if errors.As(err, &coder) && coder.Code() == 22922 || strings.Contains(err.Error(), "invalid dpiLob handle") {
			return 0, nil
		}
		return int64(n), fmt.Errorf("getSize: %w", err)
	}
	return int64(n), nil
}

// Trim the LOB to the given size.
func (dl *DirectLob) Trim(size int64) error {
	if err := dl.drv.checkExec(func() C.int {
		return C.dpiLob_trim(dl.dpiLob, C.uint64_t(size))
	}); err != nil {
		return fmt.Errorf("trim: %w", err)
	}
	return nil
}

// Set the contents of the LOB to the given byte slice.
// The LOB is cleared first.
func (dl *DirectLob) Set(p []byte) error {
	var ptr *C.char
	if len(p) != 0 {
		ptr = (*C.char)(unsafe.Pointer(&p[0]))
	}
	if err := dl.drv.checkExec(func() C.int {
		return C.dpiLob_setFromBytes(dl.dpiLob, ptr, C.uint64_t(len(p)))
	}); err != nil {
		return fmt.Errorf("setFromBytes: %w", err)
	}
	return nil
}

// ReadAt reads at most len(p) bytes into p at offset.
//
// CLOB's offset must be in amount of characters, and does not work reliably!
//
// WARNING: for historical reasons, Oracle stores CLOBs and NCLOBs using the UTF-16 encoding,
// regardless of what encoding is otherwise in use by the database.
// The number of characters, however, is defined by the number of UCS-2 codepoints.
// For this reason, if a character requires more than one UCS-2 codepoint,
// the size returned will be inaccurate and care must be taken to account for the difference!
func (dl *DirectLob) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		panic("ReadAt into an empty buffer")
	}
	n := C.uint64_t(len(p))
	if dl.dpiLob == nil {
		return 0, io.EOF
	}
	amount := n
	if dl.isClob {
		amount /= 4
		if amount == 0 {
			return 0, io.ErrShortBuffer
		}
	}
	if err := dl.drv.checkExec(func() C.int {
		return C.dpiLob_readBytes(dl.dpiLob, C.uint64_t(offset)+1, n, (*C.char)(unsafe.Pointer(&p[0])), &n)
	}); err != nil {
		return int(n), fmt.Errorf("readBytes: %w", err)
	}
	return int(n), nil
}

// WriteAt writes p starting at offset.
func (dl *DirectLob) WriteAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if !dl.opened {
		if err := dl.drv.checkExecNoLOT(func() C.int {
			return C.dpiLob_openResource(dl.dpiLob)
		}); err != nil {
			return 0, fmt.Errorf("openResources(%p): %w", dl.dpiLob, err)
		}
		dl.opened = true
	}

	n := C.uint64_t(len(p))
	if err := dl.drv.checkExecNoLOT(func() C.int {
		return C.dpiLob_writeBytes(dl.dpiLob, C.uint64_t(offset)+1, (*C.char)(unsafe.Pointer(&p[0])), n)
	}); err != nil {
		return int(n), fmt.Errorf("writeBytes: %w", err)
	}
	return int(n), nil
}

// GetFileName Return directory alias and file name for a BFILE type LOB.
func (dl *DirectLob) GetFileName() (dir, file string, err error) {
	var directoryAliasLength, fileNameLength C.uint32_t
	var directoryAlias, fileName *C.char
	if err := dl.drv.checkExec(func() C.int {
		return C.dpiLob_getDirectoryAndFileName(dl.dpiLob,
			&directoryAlias,
			&directoryAliasLength,
			&fileName,
			&fileNameLength,
		)
	}); err != nil {
		return dir, file, fmt.Errorf("GetFileName: %w", err)
	}
	dir = C.GoStringN(directoryAlias, C.int(directoryAliasLength))
	file = C.GoStringN(fileName, C.int(fileNameLength))
	return dir, file, nil
}

func maxI(a, b int) int {
	if a < b {
		return b
	}
	return a
}
