package timeboundmap

import (
	"sync"
)

type segment struct {
	sync.RWMutex
	bucket map[interface{}]*extValue
}

func (s *segment) set(key interface{}, value *extValue) {
	s.Lock()
	s.bucket[key] = value
	s.Unlock()
}

func (s *segment) unsafeSet(key interface{}, value *extValue) {
	s.bucket[key] = value
}

func (s *segment) get(key interface{}) (extVal *extValue, ok bool) {
	s.RLock()
	extVal, ok = s.bucket[key]
	s.RUnlock()
	return
}

func (s *segment) unsafeGet(key interface{}) (extVal *extValue, ok bool) {
	extVal, ok = s.bucket[key]
	return
}

func (s *segment) remove(key interface{}, value *extValue) {
	if value.cb != nil {
		go func(cb CallbackFunc, k, v interface{}) {
			cb(k, v)
		}(value.cb, key, value.val)
	}

	s.Lock()
	delete(s.bucket, key)
	s.Unlock()
}

func (s *segment) unsafeRemove(key interface{}, value *extValue) {
	if value.cb != nil {
		go func(cb CallbackFunc, k, v interface{}) {
			cb(k, v)
		}(value.cb, key, value.val)
	}

	delete(s.bucket, key)
}
