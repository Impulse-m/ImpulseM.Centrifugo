package metrics

import (
	"sync/atomic"
)

// boundReader is a value which a gauge func reads at scrape time. A component binds its reader once it runs and unbinds
// it when it stops, so a scrape reads the live value and no component writes a gauge on its hot path. The zero value is
// unbound and reads the zero value of T.
type boundReader[T any] struct {
	read atomic.Pointer[func() T]
}

// Bind makes Read call read. A nil read unbinds the reader.
func (r *boundReader[T]) Bind(read func() T) {
	if read == nil {
		r.read.Store(nil)
		return
	}
	r.read.Store(&read)
}

// Read returns the value of the bound reader, or the zero value of T while unbound.
func (r *boundReader[T]) Read() T {
	read := r.read.Load()
	if read == nil {
		var zero T
		return zero
	}
	return (*read)()
}
