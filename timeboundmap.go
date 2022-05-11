package timeboundmap

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

const defaultSegmentCount = 16

var _ TimeBoundMap = (*timeBoundMap)(nil)

type timeBoundMap struct {
	segments []*segment
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
	if n <= 0 {
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

func New(cleanInterval time.Duration, opts ...Option) TimeBoundMap {
	tbm := &timeBoundMap{
		hashPool: &sync.Pool{New: func() any { return new(maphash.Hash) }},
		seed:     maphash.MakeSeed(),
		opt:      defaultOption(),
	}
	for _, fn := range opts {
		if fn == nil {
			continue
		}
		fn(tbm.opt)
	}

	tbm.segments = make([]*segment, tbm.opt.segmentSize)
	for i := range tbm.segments {
		tbm.segments[i] = &segment{bucket: make(map[any]*extValue)}
	}

	go tbm.startRemover(cleanInterval)

	return tbm
}

func (tbm *timeBoundMap) Set(key, value any, lifetime time.Duration, onCleaned ...CallbackFunc) {
	var (
		expiration = time.Now().Add(lifetime)
		cb         CallbackFunc
		s          = tbm.getSegment(key)
	)
	if len(onCleaned) > 0 {
		cb = onCleaned[0]
	}

	ev := newExtValue(value, expiration, cb)

	s.set(key, ev)
}

func (tbm *timeBoundMap) UnsafeSet(key, value any, lifetime time.Duration, onCleaned ...CallbackFunc) {
	var (
		expiration = time.Now().Add(lifetime)
		cb         CallbackFunc
		s          = tbm.getSegment(key)
	)
	if len(onCleaned) > 0 {
		cb = onCleaned[0]
	}

	ev := newExtValue(value, expiration, cb)

	s.unsafeSet(key, ev)
}

func (tbm *timeBoundMap) Get(key any) (value any, ok bool) {
	var s = tbm.getSegment(key)

	extVal, ok := s.get(key)
	if !ok {
		return nil, false
	}

	if time.Now().After(extVal.expiration) {
		s.remove(key, extVal)
		return nil, false
	}

	return extVal.val, ok
}

func (tbm *timeBoundMap) UnsafeGet(key any) (value any, ok bool) {
	var s = tbm.getSegment(key)

	extVal, ok := s.unsafeGet(key)
	if !ok {
		return nil, false
	}

	if time.Now().After(extVal.expiration) {
		s.remove(key, extVal)
		return nil, false
	}

	return extVal.val, ok
}

func (tbm *timeBoundMap) GetToDoWithLock(key any, do func(value any, ok bool)) {
	var s = tbm.getSegment(key)

	s.Lock()
	defer s.Unlock()

	extVal, ok := s.bucket[key]
	if !ok {
		do(nil, ok)
		return
	}

	if time.Now().After(extVal.expiration) {
		s.unsafeRemove(key, extVal)
		do(nil, false)
		return
	}

	do(extVal.val, true)
}

func (tbm *timeBoundMap) getSegment(key any) *segment {
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

func (tbm *timeBoundMap) Len() int {
	var (
		count uint64
		wg    sync.WaitGroup
	)
	wg.Add(len(tbm.segments))

	for i := range tbm.segments {
		go func(s *segment) {
			defer wg.Done()
			atomic.AddUint64(&count, uint64(len(s.bucket)))
		}(tbm.segments[i])
	}

	wg.Wait()

	return int(count)
}

func (tbm *timeBoundMap) Snapshot() map[any]any {
	m := make(map[any]any, 1024)
	for _, s := range tbm.segments {
		s.RLock()
		for k, extVal := range s.bucket {
			m[k] = extVal.val
		}
		s.RUnlock()
	}
	return m
}

func (tbm *timeBoundMap) startRemover(cleanInterval time.Duration) {
	ticker := time.NewTicker(cleanInterval)
	for {
		select {
		case <-ticker.C:
			tbm.remove()
		}
	}
}

func (tbm *timeBoundMap) remove() {
	now := time.Now()

	var (
		wg                 sync.WaitGroup
		removed, remaining uint64 = 0, 0
		start                     = time.Now()
	)
	wg.Add(len(tbm.segments))

	for i := range tbm.segments {
		go func(s *segment) {
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
