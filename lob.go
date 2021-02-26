// Copyright 2017, 2020 The Godror Authors
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
	"fmt"
	"io"
	"runtime"
	"sync"
	"unicode/utf8"
	"unsafe"

	errors "golang.org/x/xerrors"
)

// Lob is for reading/writing a LOB.
type Lob struct {
	io.Reader
	IsClob bool
}

// Hijack the underlying lob reader/writer, and
// return a DirectLob for reading/writing the lob directly.
//
// After this, the Lob is unusable!
func (lob *Lob) Hijack() (*DirectLob, error) {
	if lob == nil || lob.Reader == nil {
		return nil, errors.New("lob is nil")
	}
	lr, ok := lob.Reader.(*dpiLobReader)
	if !ok {
		return nil, fmt.Errorf("Lob.Reader is %T, not *dpiLobReader", lob.Reader)
	}
	lob.Reader = nil
	return &DirectLob{conn: lr.conn, dpiLob: lr.dpiLob}, nil
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

// Scan assigns a value from a database driver.
//
// The src value will be of one of the following types:
//
//    int64
//    float64
//    bool
//    []byte
//    string
//    time.Time
//    nil - for NULL values
//
// An error should be returned if the value cannot be stored
// without loss of information.
func (dlr *dpiLobReader) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot convert LOB to %T", src)
	}
	_ = b
	return nil
}

var _ = io.Reader((*dpiLobReader)(nil))

type dpiLobReader struct {
	*conn
	dpiLob              *C.dpiLob
	offset, sizePlusOne C.uint64_t
	mu                  sync.Mutex
	chunkSize           C.uint32_t
	finished            bool
	IsClob              bool
}

var freeBufs = make(chan []byte, 4)

// WriteTo writes data to w until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written. Any error encountered during the write is also returned.
//
// Uses efficient, multiple-of-LOB-chunk-size buffered reads.
func (dlr *dpiLobReader) WriteTo(w io.Writer) (n int64, err error) {
	size := dlr.ChunkSize()
	for size < 1<<19 { // at most 1M
		size *= 2
	}
	return io.CopyBuffer(w, io.Reader(dlr), make([]byte, size))
}

// ChunkSize returns the LOB's native chunk size. Reads/writes with a multiply of this size is the most performant.
func (dlr *dpiLobReader) ChunkSize() int {
	if dlr.chunkSize != 0 {
		return int(dlr.chunkSize)
	}
	runtime.LockOSThread()
	ok := C.dpiLob_getChunkSize(dlr.dpiLob, &dlr.chunkSize) != C.DPI_FAILURE
	defer runtime.UnlockOSThread()
	if !ok {
		dlr.chunkSize = 0
		return -1
	}
	return int(dlr.chunkSize)
}

func (dlr *dpiLobReader) Read(p []byte) (int, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if dlr == nil {
		return 0, errors.New("read on nil dpiLobReader")
	}
	dlr.mu.Lock()
	defer dlr.mu.Unlock()
	if Log != nil {
		Log("msg", "LOB Read", "dlr", fmt.Sprintf("%p", dlr), "offset", dlr.offset, "size", dlr.sizePlusOne, "finished", dlr.finished, "clob", dlr.IsClob)
	}
	if dlr.finished {
		return 0, io.EOF
	}
	if len(p) < 1 || dlr.IsClob && len(p) < 4 {
		return 0, io.ErrShortBuffer
	}
	// For CLOB, sizePlusOne and offset counts the CHARACTERS!
	// See https://oracle.github.io/odpi/doc/public_functions/dpiLob.html dpiLob_readBytes
	if dlr.sizePlusOne == 0 {
		// never read size before
		if C.dpiLob_getSize(dlr.dpiLob, &dlr.sizePlusOne) == C.DPI_FAILURE {
			err := fmt.Errorf("getSize: %w", dlr.getError())
			C.dpiLob_close(dlr.dpiLob)
			dlr.dpiLob = nil
			return 0, err
		}
		dlr.sizePlusOne++
	}
	n := C.uint64_t(len(p))
	// fmt.Printf("%p.Read offset=%d sizePlusOne=%d n=%d\n", dlr.dpiLob, dlr.offset, dlr.sizePlusOne, n)
	if Log != nil {
		Log("msg", "Read", "offset", dlr.offset, "sizePlusOne", dlr.sizePlusOne, "n", n)
	}
	if dlr.offset+1 >= dlr.sizePlusOne {
		if Log != nil {
			Log("msg", "LOB reached end", "offset", dlr.offset, "size", dlr.sizePlusOne)
		}
		return 0, io.EOF
	}
	if C.dpiLob_readBytes(dlr.dpiLob, dlr.offset+1, n, (*C.char)(unsafe.Pointer(&p[0])), &n) == C.DPI_FAILURE {
		if Log != nil {
			Log("msg", "readBytes", "error", dlr.getError())
		}
		if err := dlr.getError(); err != nil {
			C.dpiLob_close(dlr.dpiLob)
			dlr.dpiLob = nil
			if Log != nil {
				Log("msg", "LOB read", "error", err)
			}
			var codeErr interface{ Code() int }
			if dlr.finished = errors.As(err, &codeErr) && codeErr.Code() == 1403; dlr.finished {
				dlr.offset += n
				return int(n), io.EOF
			}
			return int(n), fmt.Errorf("dpiLob_readbytes(lob=%p offset=%d n=%d): %w", dlr.dpiLob, dlr.offset, len(p), err)
		}
	}
	if Log != nil {
		Log("msg", "read", "n", n)
	}
	if dlr.IsClob {
		dlr.offset += C.uint64_t(utf8.RuneCount(p[:n]))
	} else {
		dlr.offset += n
	}
	var err error
	if dlr.offset+1 >= dlr.sizePlusOne {
		C.dpiLob_close(dlr.dpiLob)
		dlr.dpiLob = nil
		dlr.finished = true
		err = io.EOF
	}
	if Log != nil {
		Log("msg", "LOB", "n", n, "offset", dlr.offset, "size", dlr.sizePlusOne, "finished", dlr.finished, "clob", dlr.IsClob, "error", err)
	}
	return int(n), err
}

type dpiLobWriter struct {
	*conn
	dpiLob *C.dpiLob
	offset C.uint64_t
	opened bool
	isClob bool
}

func (dlw *dpiLobWriter) Write(p []byte) (int, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	lob := dlw.dpiLob
	if !dlw.opened {
		// fmt.Printf("open %p\n", lob)
		if C.dpiLob_openResource(lob) == C.DPI_FAILURE {
			return 0, fmt.Errorf("openResources(%p): %w", lob, dlw.getError())
		}
		dlw.opened = true
	}

	n := C.uint64_t(len(p))
	if C.dpiLob_writeBytes(lob, dlw.offset+1, (*C.char)(unsafe.Pointer(&p[0])), n) == C.DPI_FAILURE {
		if err := dlw.getError(); err != nil {
			err = fmt.Errorf("writeBytes(%p, offset=%d, data=%d): %w", lob, dlw.offset, n, err)
			dlw.dpiLob = nil
			closeLob(dlw, lob)
			return 0, err
		}
	}
	// fmt.Printf("written %q into %p@%d\n", p[:n], lob, dlw.offset)
	dlw.offset += n

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
	return nil
}

// DirectLob holds a Lob and allows direct (Read/WriteAt, not streaming Read/Write) operations on it.
type DirectLob struct {
	conn   *conn
	dpiLob *C.dpiLob
	opened bool
}

var _ = io.ReaderAt((*DirectLob)(nil))
var _ = io.WriterAt((*DirectLob)(nil))

// NewTempLob returns a temporary LOB as DirectLob.
func (c *conn) NewTempLob(isClob bool) (*DirectLob, error) {
	typ := C.uint(C.DPI_ORACLE_TYPE_BLOB)
	if isClob {
		typ = C.DPI_ORACLE_TYPE_CLOB
	}
	lob := DirectLob{conn: c}
	if err := c.checkExec(func() C.int { return C.dpiConn_newTempLob(c.dpiConn, typ, &lob.dpiLob) }); err != nil {
		return nil, fmt.Errorf("newTempLob: %w", err)
	}
	return &lob, nil
}

// Close the Lob.
func (dl *DirectLob) Close() error {
	if !dl.opened {
		return nil
	}
	lob := dl.dpiLob
	dl.opened, dl.dpiLob = false, nil
	return closeLob(dl.conn, lob)
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
	if err := dl.conn.checkExec(func() C.int { return C.dpiLob_getSize(dl.dpiLob, &n) }); err != nil {
		return int64(n), fmt.Errorf("getSize: %w", err)
	}
	return int64(n), nil
}

// Trim the LOB to the given size.
func (dl *DirectLob) Trim(size int64) error {
	if err := dl.conn.checkExec(func() C.int { return C.dpiLob_trim(dl.dpiLob, C.uint64_t(size)) }); err != nil {
		return fmt.Errorf("trim: %w", err)
	}
	return nil
}

// Set the contents of the LOB to the given byte slice.
// The LOB is cleared first.
func (dl *DirectLob) Set(p []byte) error {
	if err := dl.conn.checkExec(func() C.int {
		return C.dpiLob_setFromBytes(dl.dpiLob, (*C.char)(unsafe.Pointer(&p[0])), C.uint64_t(len(p)))
	}); err != nil {
		return fmt.Errorf("setFromBytes: %w", err)
	}
	return nil
}

// ReadAt reads at most len(p) bytes into p at offset.
func (dl *DirectLob) ReadAt(p []byte, offset int64) (int, error) {
	n := C.uint64_t(len(p))
	if err := dl.conn.checkExec(func() C.int {
		return C.dpiLob_readBytes(dl.dpiLob, C.uint64_t(offset)+1, n, (*C.char)(unsafe.Pointer(&p[0])), &n)
	}); err != nil {
		return int(n), fmt.Errorf("readBytes: %w", err)
	}
	return int(n), nil
}

// WriteAt writes p starting at offset.
func (dl *DirectLob) WriteAt(p []byte, offset int64) (int, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if !dl.opened {
		// fmt.Printf("open %p\n", lob)
		if C.dpiLob_openResource(dl.dpiLob) == C.DPI_FAILURE {
			return 0, fmt.Errorf("openResources(%p): %w", dl.dpiLob, dl.conn.getError())
		}
		dl.opened = true
	}

	n := C.uint64_t(len(p))
	if C.dpiLob_writeBytes(dl.dpiLob, C.uint64_t(offset)+1, (*C.char)(unsafe.Pointer(&p[0])), n) == C.DPI_FAILURE {
		return int(n), fmt.Errorf("writeBytes: %w", dl.conn.getError())
	}
	return int(n), nil
}

// GetFileName Return directory alias and file name for a BFILE type LOB.
func (dl *DirectLob) GetFileName() (dir, file string, err error) {
	var directoryAliasLength, fileNameLength C.uint32_t
	var directoryAlias, fileName *C.char
	if err := dl.conn.checkExec(func() C.int {
		return C.dpiLob_getDirectoryAndFileName(dl.dpiLob,
			&directoryAlias,
			&directoryAliasLength,
			&fileName,
			&fileNameLength,
		)
	}); err != nil {
		return dir, file, errors.Errorf("GetFileName: %w", err)
	}
	dir = C.GoStringN(directoryAlias, C.int(directoryAliasLength))
	file = C.GoStringN(fileName, C.int(fileNameLength))
	return dir, file, nil
}
