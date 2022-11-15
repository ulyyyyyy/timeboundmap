package timeboundmap

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

const defaultSegmentCount = 16

type timeBoundMap[K comparable, V any] struct {
	segments []*segment[K, V]
	hashPool *sync.Pool
	seed     maphash.Seed
	opt      *option
}

type Option func(*option)

func defaultOption() *option {
	return &option{
		segmentSize:    defaultSegmentCount,
		fnOnClearingUp: nil,
	}
}

type option struct {
	segmentSize    uint64
	fnOnClearingUp func(elapsed time.Duration, removed, remaining uint64)
}

func WithSegmentSize(n int) Option {
	if n <= 0 || n%2 != 0 {
		n = defaultSegmentCount
	}
	return func(opt *option) {
		opt.segmentSize = uint64(n)
	}
}

func WithOnClearingUp(fn func(elapsed time.Duration, removed, remaining uint64)) Option {
	return func(opt *option) {
		opt.fnOnClearingUp = fn
	}
}

func New[K comparable, V any](cleanInterval time.Duration, opts ...Option) TimeBoundMap[K, V] {
	tbm := &timeBoundMap[K, V]{
		hashPool: &sync.Pool{New: func() interface{} { return new(maphash.Hash) }},
		seed:     maphash.MakeSeed(),
		opt:      defaultOption(),
	}
	for _, fn := range opts {
		if fn == nil {
			continue
		}
		fn(tbm.opt)
	}

	tbm.segments = make([]*segment[K, V], tbm.opt.segmentSize)
	for i := range tbm.segments {
		tbm.segments[i] = &segment[K, V]{bucket: make(map[K]*extValue[V])}
	}

	go tbm.startRemover(cleanInterval)

	return tbm
}

func (tbm *timeBoundMap[K, V]) Set(key K, value V, lifetime time.Duration, onCleaned ...CallbackFunc) {
	var (
		expiration = time.Now().Add(lifetime)
		cb         CallbackFunc
		s          = tbm.getSegment(key)
	)
	if len(onCleaned) > 0 {
		cb = onCleaned[0]
	}

	ev := newExtValue[V](value, expiration, cb)

	s.set(key, ev)
}

func (tbm *timeBoundMap[K, V]) UnsafeSet(key K, value V, lifetime time.Duration, onCleaned ...CallbackFunc) {
	var (
		expiration = time.Now().Add(lifetime)
		cb         CallbackFunc
		s          = tbm.getSegment(key)
	)
	if len(onCleaned) > 0 {
		cb = onCleaned[0]
	}

	ev := newExtValue[V](value, expiration, cb)

	s.unsafeSet(key, ev)
}

func (tbm *timeBoundMap[K, V]) Get(key K) (value V, ok bool) {
	var s = tbm.getSegment(key)

	extVal, ok := s.get(key)
	if !ok {
		return
	}

	if time.Now().After(extVal.expiration) {
		s.remove(key, extVal)
		return
	}

	return extVal.val, ok
}

func (tbm *timeBoundMap[K, V]) UnsafeGet(key K) (value V, ok bool) {
	var s = tbm.getSegment(key)

	extVal, ok := s.unsafeGet(key)
	if !ok {
		return
	}

	if time.Now().After(extVal.expiration) {
		s.remove(key, extVal)
		return
	}

	return extVal.val, ok
}

func (tbm *timeBoundMap[K, V]) GetToDoWithLock(key K, do func(value V, ok bool)) {
	var s = tbm.getSegment(key)

	s.Lock()
	defer s.Unlock()

	extVal, ok := s.bucket[key]
	if !ok {
		var zero V
		do(zero, ok)
		return
	}

	if time.Now().After(extVal.expiration) {
		s.unsafeRemove(key, extVal)
		var zero V
		do(zero, false)
		return
	}

	do(extVal.val, true)
}

func (tbm *timeBoundMap[K, V]) getSegment(key K) *segment[K, V] {
	h := tbm.hashPool.Get().(*maphash.Hash)
	h.SetSeed(tbm.seed)
	defer func() {
		h.Reset()
		tbm.hashPool.Put(h)
	}()

	// never fails
	_, _ = h.Write(extKey{val: key}.Bytes())

	idx := h.Sum64() % tbm.opt.segmentSize
	return tbm.segments[idx]
}

func (tbm *timeBoundMap[K, V]) Len() int {
	var (
		count uint64
		wg    sync.WaitGroup
	)
	wg.Add(len(tbm.segments))

	for i := range tbm.segments {
		go func(s *segment[K, V]) {
			defer wg.Done()
			atomic.AddUint64(&count, uint64(len(s.bucket)))
		}(tbm.segments[i])
	}

	wg.Wait()

	return int(count)
}

func (tbm *timeBoundMap[K, V]) Snapshot() map[K]V {
	m := make(map[K]V, 1024)
	for _, s := range tbm.segments {
		s.RLock()
		for k, extVal := range s.bucket {
			m[k] = extVal.val
		}
		s.RUnlock()
	}
	return m
}

func (tbm *timeBoundMap[K, V]) startRemover(cleanInterval time.Duration) {
	ticker := time.NewTicker(cleanInterval)
	for {
		select {
		case <-ticker.C:
			tbm.remove()
		}
	}
}

func (tbm *timeBoundMap[K, V]) remove() {
	now := time.Now()

	var (
		wg                 sync.WaitGroup
		removed, remaining uint64 = 0, 0
		start                     = time.Now()
	)
	wg.Add(len(tbm.segments))

	for i := range tbm.segments {
		go func(s *segment[K, V]) {
			defer wg.Done()

			s.Lock()

			for k, v := range s.bucket {
				if now.After(v.expiration) {
					atomic.AddUint64(&removed, 1)
					s.unsafeRemove(k, v)
				} else {
					atomic.AddUint64(&remaining, 1)
				}
			}

			s.Unlock()
		}(tbm.segments[i])
	}

	wg.Wait()

	if tbm.opt.fnOnClearingUp != nil {
		go func() {
			tbm.opt.fnOnClearingUp(time.Now().Sub(start), atomic.LoadUint64(&removed), atomic.LoadUint64(&remaining))
		}()
	}
}
