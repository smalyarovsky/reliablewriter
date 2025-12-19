package reliablewriter

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

type UnreliableWriter interface {
	WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, buf *SGBuffer, off int64) error
	GetResumeOffset(ctx context.Context, chunkBegin, chunkEnd int64) (int64, error)
	SetObjectSize(size int64) error
	Complete(ctx context.Context) error
	Abort(ctx context.Context)
}

type FileUnreliableWriter struct {
	mu       sync.Mutex
	f        *os.File
	size     int64
	failProb float64
	rnd      *rand.Rand

	aborted   bool
	completed bool
}

func NewFileUnreliableWriter(path string, failProb float64) (*FileUnreliableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileUnreliableWriter{
		f:        f,
		failProb: failProb,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (w *FileUnreliableWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, buf *SGBuffer, off int64) (err error) {
	defer func() {
		fmt.Printf("WriteAt %d:%d; offset is %d, result is %v\n", chunkBegin, chunkEnd, off, err)
	}()
	if err := ctx.Err(); err != nil {
		return err
	}

	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return errors.New("unreliable writer aborted")
	}
	if w.completed {
		w.mu.Unlock()
		return errors.New("unreliable writer completed")
	}
	w.mu.Unlock()

	total := buf.Len()
	if total == 0 {
		return nil
	}

	toWrite := int64(total)
	partialFail := false
	if w.rnd.Float64() < w.failProb && toWrite > 0 {
		partialFail = true
		if toWrite > 1 {
			toWrite = 1 + w.rnd.Int63n(toWrite-1)
		} else {
			toWrite = 1
		}
	}

	n, err := writeFromSGBufferAt(w.f, buf, off, toWrite)
	if err != nil {
		return err
	}

	end := off + n

	w.mu.Lock()
	if end > w.size {
		w.size = end
	}
	w.mu.Unlock()

	if partialFail {
		return errors.New("simulated failure")
	}
	return nil
}

func (w *FileUnreliableWriter) GetResumeOffset(ctx context.Context, chunkBegin, chunkEnd int64) (x int64, err error) {
	defer func() {
		fmt.Printf("GetResumeOffset %d:%d; offset is %d; err is %v\n", chunkBegin, chunkEnd, x, err)
	}()
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if w.rnd.Float64() < w.failProb {
		return 0, errors.New("simulated failure")
	}

	w.mu.Lock()
	size := w.size
	aborted := w.aborted
	completed := w.completed
	w.mu.Unlock()

	if aborted {
		return 0, errors.New("unreliable writer aborted")
	}
	if completed {
		return 0, errors.New("unreliable writer completed")
	}

	if size <= chunkBegin {
		return 0, nil
	}
	if size >= chunkEnd {
		return chunkEnd - chunkBegin, nil
	}
	return size - chunkBegin, nil
}

func (w *FileUnreliableWriter) SetObjectSize(size int64) (err error) {
	defer func() {
		fmt.Printf("SetObjectSize %d; result is %v\n", size, err)
	}()
	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return errors.New("unreliable writer aborted")
	}
	w.size = size
	f := w.f
	w.mu.Unlock()

	return f.Truncate(size)
}

func (w *FileUnreliableWriter) Complete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return errors.New("unreliable writer aborted")
	}
	if w.completed {
		w.mu.Unlock()
		return nil
	}
	w.completed = true
	f := w.f
	w.mu.Unlock()

	return f.Close()
}

func (w *FileUnreliableWriter) Abort(ctx context.Context) {
	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return
	}
	w.aborted = true
	f := w.f
	w.mu.Unlock()

	_ = f.Close()
}

func writeFromSGBufferAt(f *os.File, buf *SGBuffer, off int64, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}

	remaining := limit
	curOff := off
	var written int64

	for _, seg := range buf.bufs {
		if remaining <= 0 {
			break
		}
		if len(seg) == 0 {
			continue
		}

		take := int64(len(seg))
		if take > remaining {
			take = remaining
		}

		n, err := f.WriteAt(seg[:take], curOff)
		written += int64(n)
		curOff += int64(n)
		remaining -= int64(n)

		if err != nil {
			return written, err
		}
		if int64(n) < take {
			break
		}
	}

	return written, nil
}
