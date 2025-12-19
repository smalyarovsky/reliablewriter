package reliablewriter

type SGBuffer struct {
	bufs [][]byte
	size int64
}

func NewSGBuffer() *SGBuffer {
	return &SGBuffer{}
}

func (b *SGBuffer) Len() int64 {
	return b.size
}

func (b *SGBuffer) Append(p []byte) {
	if len(p) == 0 {
		return
	}
	b.bufs = append(b.bufs, p)
	b.size += int64(len(p))
}

func (b *SGBuffer) Reset() {
	b.bufs = b.bufs[:0]
	b.size = 0
}

func (b *SGBuffer) Splice(other *SGBuffer) {
	if other == nil || other.size == 0 {
		return
	}
	b.bufs = append(b.bufs, other.bufs...)
	b.size += other.size
	other.Reset()
}

func (b *SGBuffer) Take(n int64) *SGBuffer {
	if n <= 0 {
		return NewSGBuffer()
	}
	if n > b.size {
		n = b.size
	}
	res := NewSGBuffer()
	remaining := n
	i := 0

	for ; i < len(b.bufs) && remaining > 0; i++ {
		seg := b.bufs[i]
		segLen := int64(len(seg))

		if segLen <= remaining {
			res.bufs = append(res.bufs, seg)
			remaining -= segLen
		} else {
			res.bufs = append(res.bufs, seg[:remaining])
			b.bufs[i] = seg[remaining:]
			remaining = 0
			break
		}
	}

	b.bufs = b.bufs[i:]
	b.size -= n
	res.size = n

	return res
}

func (b *SGBuffer) Skip(n int64) {
	if n <= 0 {
		return
	}
	if n >= b.size {
		b.Reset()
		return
	}
	remaining := n
	i := 0

	for ; i < len(b.bufs) && remaining > 0; i++ {
		seg := b.bufs[i]
		segLen := int64(len(seg))

		if segLen <= remaining {
			remaining -= segLen
			continue
		} else {
			b.bufs[i] = seg[remaining:]
			break
		}
	}
	b.bufs = b.bufs[i:]
	b.size -= n
}
