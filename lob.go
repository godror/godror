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

// ErrLOBClosed is returned when a Lob is used after it has been closed.
//
// A Lob returned from a row is closed by the next Next or Close of the rows,
// and a Lob returned as an OUT parameter is closed when its statement
// is executed again or closed;
// a Lob read from an Object is closed when the Object is closed.
var ErrLOBClosed = errors.New("LOB is closed")

// Lob is for reading/writing a LOB.
//
// A Lob that the database returned is valid only for a limited time:
// when it is a column of a row (see LobAsReader), till the next Next or Close of the rows
// (so it must be read before sql.Row.Scan returns - use Query, not QueryRow);
// when it is an OUT parameter, till the statement is executed again or closed;
// when it is an attribute or collection element, till its Object is closed.
// After that, it returns ErrLOBClosed.
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
//
// The DirectLob is valid only as long as the Lob would have been (see Lob):
// it is closed at that point, if it has not been closed before.
func (lob *Lob) Hijack() (*DirectLob, error) {
	if lob == nil || lob.Reader == nil {
		return nil, errors.New("lob is nil")
	}
	lr, ok := lob.Reader.(*dpiLobReader)
	if !ok {
		return nil, fmt.Errorf("Lob.Reader is %T, not *dpiLobReader", lob.Reader)
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if lr.closed {
		return nil, ErrLOBClosed
	}
	lob.Reader = nil
	dl := &DirectLob{drv: lr.drv, dpiLob: lr.dpiLob, owned: lr.owned}
	if lr.owned {
		// The reference moves to the DirectLob,
		// which is closed with the reader, at the latest.
		lr.dpiLob, lr.owned, lr.hijacked = nil, false, dl
	}
	return dl, nil
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
	// owned is true when the reader holds its own reference to dpiLob.
	owned bool
	// closed is set by Close and release: the LOB must not be used anymore.
	closed bool
	// hijacked is the DirectLob that took over the reader's reference.
	hijacked *DirectLob
}

// addRef takes a reference to the LOB for the reader, as the variable
// that holds the LOB frees it at the next fetch or when it is released.
func (dlr *dpiLobReader) addRef() error {
	if C.dpiLob_addRef(dlr.dpiLob) == C.DPI_FAILURE {
		return fmt.Errorf("addRef LOB: %w", dlr.getError())
	}
	dlr.owned = true
	return nil
}

// release closes the reader (and the DirectLob hijacked from it)
// without a round trip: every later use returns ErrLOBClosed.
func (dlr *dpiLobReader) release() {
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	dlr.closed = true
	if dlr.hijacked != nil {
		_ = dlr.hijacked.Close()
		dlr.hijacked = nil
	}
	dlr.dropLob()
}

// done reports whether the reader holds no LOB anymore.
func (dlr *dpiLobReader) done() bool {
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	return dlr.dpiLob == nil && dlr.hijacked == nil
}

// rewind reports whether the reader is open and reads lob;
// if so, the next Read starts at the beginning of the LOB.
func (dlr *dpiLobReader) rewind(lob *C.dpiLob) bool {
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	if dlr.closed || dlr.dpiLob != lob {
		return false
	}
	dlr.offset, dlr.sizePlusOne, dlr.bufR, dlr.bufW, dlr.finished = 0, 0, 0, 0, false
	return true
}

// dropLob forgets the LOB, releasing the reader's reference, if it has one.
func (dlr *dpiLobReader) dropLob() {
	if dlr.owned && dlr.dpiLob != nil {
		C.dpiLob_release(dlr.dpiLob)
	}
	dlr.dpiLob, dlr.owned = nil, false
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
	if dlr.closed {
		return 0, ErrLOBClosed
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
	if dlr.closed {
		dlr.mu.Unlock()
		return 0, ErrLOBClosed
	}
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
	if dlr.sizePlusOne != 0 {
		return nil
	}
	var err error
	runtime.LockOSThread()
	if err = dlr.checkExecNoLOT(func() C.int {
		return C.dpiLob_getSize(dlr.dpiLob, &dlr.sizePlusOne)
	}); err != nil {
		err = fmt.Errorf("getSize: %w", err)
		C.dpiLob_close(dlr.dpiLob)
		dlr.dropLob()
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
			C.dpiLob_close(dlr.dpiLob)
			dlr.dropLob()
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
		// Trim the last erroneous encodings: an invalid or incomplete
		// sequence is decoded as RuneError with size 1.
		// A properly encoded U+FFFD (size 3) is data, and must be kept.
		for {
			r, size := utf8.DecodeLastRune(p[:n])
			if r != utf8.RuneError || size != 1 {
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
		C.dpiLob_close(dlr.dpiLob)
		dlr.dropLob()
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
	if dlr.closed {
		return 0, ErrLOBClosed
	}
	if dlr.IsClob {
		return 0, ErrCLOB
	}
	return readLobAt(dlr.drv, dlr.dpiLob, p, off)
}
func (dlr *dpiLobReader) Close() error {
	if dlr == nil {
		return nil
	}
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	dlr.closed = true
	if dlr.dpiLob == nil {
		return nil
	}
	lob := dlr.dpiLob
	dlr.dpiLob, dlr.owned = nil, false
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

// flush drops the remaining incomplete UTF-8 sequence of a CLOB, if any.
//
// The data ended in the middle of a character. Such a tail is no character,
// so it cannot be written alone (OCILobWrite2 crashes on it); Oracle drops it
// from a string bind, too.
func (dlw *dpiLobWriter) flush() error {
	dlw.tailLen = 0
	return nil
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
	drv    *drv
	dpiLob *C.dpiLob
	// mu protects dpiLob and opened: the rows or the statement that the
	// DirectLob was hijacked from closes it, maybe from another goroutine.
	mu             sync.Mutex
	opened, isClob bool
	// owned is true when dpiLob was created by us (NewTempLob),
	// so Close must release it even if it was never opened.
	owned bool
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
	return &lob, nil
}

// Close the Lob.
func (dl *DirectLob) Close() error {
	if dl == nil {
		return nil
	}
	dl.mu.Lock()
	defer dl.mu.Unlock()
	if dl.dpiLob == nil || !(dl.opened || dl.owned) {
		return nil
	}
	lob := dl.dpiLob
	dl.opened, dl.owned, dl.dpiLob = false, false, nil
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
	dl.mu.Lock()
	defer dl.mu.Unlock()
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
	dl.mu.Lock()
	defer dl.mu.Unlock()
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
	dl.mu.Lock()
	defer dl.mu.Unlock()
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
	dl.mu.Lock()
	defer dl.mu.Unlock()
	if dl.dpiLob == nil {
		return 0, io.EOF
	}
	if dl.isClob && len(p) < 4 {
		return 0, io.ErrShortBuffer
	}
	return readLobAt(dl.drv, dl.dpiLob, p, offset)
}

func readLobAt(d *drv, lob *C.dpiLob, p []byte, offset int64) (int, error) {
	var typ C.dpiOracleTypeNum
	n := C.uint64_t(len(p))
	if err := d.checkExec(func() C.int {
		rc := C.dpiLob_readBytes(lob, C.uint64_t(offset)+1, n, (*C.char)(unsafe.Pointer(&p[0])), &n)
		if rc == C.DPI_SUCCESS && int(n) < len(p) {
			// A short byte count indicates EOF for BLOBs. Character LOBs
			// use character offsets, and a hijacked LOB may lack Go-side type info.
			return C.dpiLob_getType(lob, &typ)
		}
		return rc
	}); err != nil {
		return int(n), fmt.Errorf("readBytes at %d: %w", offset, err)
	}
	// typ != 0 iff we had a short read - but it matter only for BLOBs.
	if typ == C.DPI_ORACLE_TYPE_BLOB {
		return int(n), io.EOF
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
	dl.mu.Lock()
	defer dl.mu.Unlock()

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
	dl.mu.Lock()
	defer dl.mu.Unlock()
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
