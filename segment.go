package timeboundmap

import (
	"sync"
)

type segment[K comparable, V any] struct {
	sync.RWMutex
	bucket map[K]*extValue[V]
}

func (s *segment[K, V]) set(key K, value *extValue[V]) {
	s.Lock()
	s.bucket[key] = value
	s.Unlock()
}

func (s *segment[K, V]) unsafeSet(key K, value *extValue[V]) {
	s.bucket[key] = value
}

func (s *segment[K, V]) get(key K) (extVal *extValue[V], ok bool) {
	s.RLock()
	extVal, ok = s.bucket[key]
	s.RUnlock()
	return
}

func (s *segment[K, V]) unsafeGet(key K) (extVal *extValue[V], ok bool) {
	extVal, ok = s.bucket[key]
	return
}

func (s *segment[K, V]) remove(key K, value *extValue[V]) {
	if value.cb != nil {
		go func(cb CallbackFunc, k K, v V) {
			cb(k, v)
		}(value.cb, key, value.val)
	}

	s.Lock()
	delete(s.bucket, key)
	s.Unlock()
}

func (s *segment[K, V]) unsafeRemove(key K, value *extValue[V]) {
	if value.cb != nil {
		go func(cb CallbackFunc, k K, v V) {
			cb(k, v)
		}(value.cb, key, value.val)
	}

	delete(s.bucket, key)
}
