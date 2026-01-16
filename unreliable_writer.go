package reliablewriter

import (
	"context"
	"fmt"
	"math"
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
	mu              sync.Mutex
	f               *os.File
	curOff          int64
	failRate        float64
	bytesBeforeFail int64
	rnd             *rand.Rand
	aborted         bool
	completed       bool
}

// Write of every byte fails with probability failRate.
func NewFileUnreliableWriter(path string, failRate float64) (*FileUnreliableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileUnreliableWriter{
		f:        f,
		failRate: failRate,
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
		return ErrAborted
	}
	if w.completed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.mu.Unlock()

	toWrite := buf.Len()
	if toWrite == 0 {
		return nil
	}
	fail := w.bytesBeforeFail < toWrite
	toWrite = min(toWrite, w.bytesBeforeFail)

	for _, seg := range buf.bufs {
		if toWrite == 0 {
			break
		}
		n, err := w.f.WriteAt(seg[:min(toWrite, int64(len(seg)))], off)
		if err != nil {
			panic(err)
		}
		toWrite -= int64(n)
		off += int64(n)
	}

	w.mu.Lock()
	if w.curOff < off {
		w.curOff = off
	}
	w.mu.Unlock()

	if fail {
		w.bytesBeforeFail = int64(math.Log(1-w.rnd.Float64()) / math.Log(1-w.failRate))
		fmt.Fprintf(os.Stderr, "%d\n", w.bytesBeforeFail)
		return ErrSimulated
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
	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return 0, ErrAborted
	}
	if w.completed {
		w.mu.Unlock()
		return 0, ErrClosed
	}
	w.mu.Unlock()

	if w.rnd.Float64() < (1 - math.Pow((1-w.failRate), 20)) {
		return 0, ErrSimulated
	}

	w.mu.Lock()
	curOff := w.curOff
	w.mu.Unlock()
	if curOff <= chunkBegin {
		return 0, nil
	}
	if curOff >= chunkEnd {
		return chunkEnd - chunkBegin, nil
	}
	return curOff - chunkBegin, nil
}

func (w *FileUnreliableWriter) SetObjectSize(size int64) (err error) {
	defer func() {
		fmt.Printf("SetObjectSize %d; result is %v\n", size, err)
	}()
	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return ErrAborted
	}
	w.curOff = size
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
		return ErrAborted
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
