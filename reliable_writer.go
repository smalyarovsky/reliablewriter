package reliablewriter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ReliableWriter struct {
	mu        sync.Mutex
	uw        UnreliableWriter
	buf       *SGBuffer
	curOff    int64
	nextOff   int64
	minChunk  int64
	maxChunk  int64
	uploading bool
	closed    bool
	err       error
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func NewReliableWriter(ctx context.Context, uw UnreliableWriter, minChunk, maxChunk int64) *ReliableWriter {
	c, cancel := context.WithCancel(ctx)
	return &ReliableWriter{
		uw:       uw,
		buf:      NewSGBuffer(),
		minChunk: minChunk,
		maxChunk: maxChunk,
		ctx:      c,
		cancel:   cancel,
	}
}

func (w *ReliableWriter) WriteAt(ctx context.Context, p []byte, off int64) error {
	if len(p) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	w.mu.Lock()

	if w.err != nil {
		defer w.mu.Unlock()
		return w.err
	}
	if w.closed {
		defer w.mu.Unlock()
		return ErrClosed
	}
	if off != w.nextOff {
		defer w.mu.Unlock()
		return fmt.Errorf("non-sequential write: expected %d, got %d", w.nextOff, off)
	}
	w.buf.Append(p)
	w.nextOff += int64(len(p))
	w.mu.Unlock()
	w.tryStartUpload()
	return nil
}

func (w *ReliableWriter) tryStartUpload() {
	w.mu.Lock()

	if w.uploading || w.err != nil {
		w.mu.Unlock()
		return
	}

	bufLen := w.buf.Len()
	if bufLen == 0 {
		w.mu.Unlock()
		return
	}

	if !w.closed && bufLen < w.minChunk {
		w.mu.Unlock()
		return
	}

	var chunkSize int64
	if w.closed {
		chunkSize = min(bufLen, w.maxChunk)
	} else {
		chunkSize = min((bufLen/w.minChunk)*w.minChunk, w.maxChunk)
	}

	if chunkSize <= 0 {
		w.mu.Unlock()
		return
	}

	chunk := w.buf.Take(chunkSize)
	chunkBegin := w.curOff
	chunkEnd := chunkBegin + chunk.Len()
	w.uploading = true

	w.mu.Unlock()

	w.wg.Go(func() {
		w.persistUpload(chunkBegin, chunkEnd, chunk)
		w.tryStartUpload()
	})
}

func (w *ReliableWriter) persistUpload(chunkBegin, chunkEnd int64, chunk *SGBuffer) {
	advance := false
	defer func() {
		w.mu.Lock()
		if advance && w.curOff < chunkEnd {
			w.curOff = chunkEnd
		}
		w.uploading = false
		w.mu.Unlock()
	}()

	localOffset := int64(0)
	maxAttempts := 10
	backoff := NewBackoff(500*time.Millisecond, 1.5, 5*time.Second)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if w.ctx.Err() != nil {
			w.setErr(w.ctx.Err())
			return
		}
		if err := w.uw.WriteAt(w.ctx, chunkBegin, chunkEnd, chunk, chunkBegin+localOffset); err == nil {
			advance = true
			return
		}
		backoffResume := NewBackoff(500*time.Millisecond, 1.5, 5*time.Second)
		resume, err := w.uw.GetResumeOffset(w.ctx, chunkBegin, chunkEnd)
		for attemptResume := 1; err != nil && attemptResume < maxAttempts; attemptResume++ {
			if w.ctx.Err() != nil {
				w.setErr(w.ctx.Err())
				return
			}
			var re *RetryableError
			if !errors.As(err, &re) {
				w.setErr(w.ctx.Err())
				return
			}
			if err := backoffResume.Wait(w.ctx); err != nil {
				w.setErr(err)
				return
			}
			resume, err = w.uw.GetResumeOffset(w.ctx, chunkBegin, chunkEnd)
		}

		if err != nil {
			break
		}

		chunkSize := chunkEnd - chunkBegin
		if resume >= chunkSize {
			advance = true
			return
		}

		if resume < localOffset {
			w.setErr(errors.New("resume offset moved backwards"))
			return
		}

		if resume > localOffset {
			chunk.Skip(resume - localOffset)
			localOffset = resume
		}

		if chunk.Len() == 0 {
			advance = true
			return
		}

		if err := backoff.Wait(w.ctx); err != nil {
			w.setErr(err)
			return
		}
	}
	w.setErr(ErrMaxRetries)
}

func (w *ReliableWriter) setErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.err == nil {
		w.err = err
		w.cancel()
	}
	w.mu.Unlock()
}

func (w *ReliableWriter) Complete(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		// TODO don't reuse flags (FORGOT WHAT IT MEANS xD)
		panic("Complete called on already closed writer")
	}
	w.closed = true
	needUpload := !w.uploading && w.buf.Len() > 0 && w.err == nil
	w.mu.Unlock()
	if needUpload {
		w.tryStartUpload()
	}

	w.wg.Wait()

	w.mu.Lock()
	err := w.err
	w.mu.Unlock()

	if err != nil {
		w.uw.Abort(ctx)
		return err
	}

	w.mu.Lock()
	if err := w.uw.SetObjectSize(w.nextOff); err != nil {
		w.mu.Unlock()
		w.uw.Abort(ctx)
		return err
	}
	w.mu.Unlock()

	return w.uw.Complete(ctx)
}

func (w *ReliableWriter) Abort(ctx context.Context) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.cancel()
	w.mu.Unlock()

	w.wg.Wait()
	w.uw.Abort(ctx)
}
