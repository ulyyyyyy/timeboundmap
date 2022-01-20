package timeboundmap

import (
	"sync"
	"sync/atomic"
	"time"
)

// TimeBoundMap
type TimeBoundMap struct {
	mu sync.RWMutex

	cursor  int
	buckets []map[extKey]*extValue

	valuePool *sync.Pool

	fnOnCleaned func(elapsed time.Duration, cleaning, remaining uint64)
}

// New make a New boundMap with expire time
func New(cleanInterval time.Duration, opts ...Option) *TimeBoundMap {
	opt := defaultOption()
	for _, fn := range opts {
		fn(opt)
	}

	buckets := make([]map[extKey]*extValue, opt.bucketTotal)
	for i := range buckets {
		buckets[i] = make(map[extKey]*extValue, opt.bucketSize)
	}

	tbm := &TimeBoundMap{
		cursor:  0,
		buckets: buckets,
		valuePool: &sync.Pool{
			New: func() interface{} {
				return new(extValue)
			},
		},
		fnOnCleaned: opt.fnOnCleaned,
	}
	go tbm.startCleaner(cleanInterval)
	return tbm
}

type option struct {
	bucketTotal int
	bucketSize  int
	fnOnCleaned func(elapsed time.Duration, cleaning, remaining uint64)
}

func defaultOption() *option {
	return &option{
		bucketTotal: 1,
		bucketSize:  256,
	}
}

type Option func(*option)

// WithBucketTotal init the number of buckets
func WithBucketTotal(n int) Option {
	if n <= 0 {
		n = 1
	}
	return func(opt *option) {
		opt.bucketTotal = n
	}
}

// WithBucketSize init the size of each bucket
func WithBucketSize(s int) Option {
	if s <= 0 {
		s = 256
	}
	return func(opt *option) {
		opt.bucketSize = s
	}
}

// WithOnCleaned init function when bucket cleaning up
func WithOnCleaned(fn func(elapsed time.Duration, cleaning, remaining uint64)) Option {
	return func(opt *option) {
		opt.fnOnCleaned = fn
	}
}

// Set the key & value into bucketMap. the callbackFunc is executed when the key is deleted.
func (tbm *TimeBoundMap) Set(key, value interface{}, lifeDuration time.Duration, cb ...CallbackFunc) {
	expiration := time.Now().Add(lifeDuration)
	var fn CallbackFunc
	if len(cb) > 0 {
		fn = cb[0]
	}

	// compute if absent
	if _, v := tbm.get(key); v != nil {
		v.value = value
		v.expiration = expiration
		v.cb = fn
		return
	}

	var k extKey

	tbm.mu.Lock()
	defer tbm.mu.Unlock()

	k.bucketIdx = tbm.cursor
	k.key = key

	maxIdx := len(tbm.buckets) - 1
	if tbm.cursor+1 > maxIdx {
		tbm.cursor = 0
	} else {
		tbm.cursor++
	}

	v := tbm.valuePool.Get().(*extValue)
	v.value = value
	v.expiration = expiration
	v.cb = fn
	tbm.buckets[k.bucketIdx][k] = v
}

// Get the value and exist result
func (tbm *TimeBoundMap) Get(key interface{}) (value interface{}, ok bool) {
	k, v := tbm.get(key)
	if v == nil {
		return nil, false
	}

	if time.Now().After(v.expiration) {
		tbm.mu.Lock()
		defer tbm.mu.Unlock()
		tbm.deleteValue(k, v)
		return nil, false
	}

	return v.value, true
}

func (tbm *TimeBoundMap) get(key interface{}) (k extKey, v *extValue) {
	var ok bool

	tbm.mu.RLock()

	fn := func() {
		for i := range tbm.buckets {
			_k := extKey{
				bucketIdx: i,
				key:       key,
			}
			_v, _ok := tbm.buckets[i][_k]
			if _ok {
				ok = true
				k = _k
				v = _v
				return
			}
		}
	}
	fn()

	tbm.mu.RUnlock()

	if !ok {
		return extKey{}, nil
	} else {
		return k, v
	}
}

// Len calc the Len for the buckets
func (tbm *TimeBoundMap) Len() int {
	var num int
	for _, bucket := range tbm.buckets {
		num += len(bucket)
	}
	return num
}

// Snapshot
func (tbm *TimeBoundMap) Snapshot() map[interface{}]interface{} {
	m := make(map[interface{}]interface{}, tbm.Len())

	tbm.mu.RLock()
	defer tbm.mu.RUnlock()

	for _, bucket := range tbm.buckets {
		for k, v := range bucket {
			m[k.key] = v.value
		}
	}

	return m
}

func (tbm *TimeBoundMap) startCleaner(d time.Duration) {
	ticker := time.NewTicker(d)

	for {
		select {
		case <-ticker.C:
			tbm.cleanup()
		}
	}
}

// cleanup clean up the expired key.
func (tbm *TimeBoundMap) cleanup() {
	now := time.Now()

	// lock buckets
	tbm.mu.Lock()
	defer tbm.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(tbm.buckets))

	var (
		count uint64
		start = time.Now()
	)
	atomic.StoreUint64(&count, 0)

	for i := range tbm.buckets {
		go func(i int) {
			for k, v := range tbm.buckets[i] {
				// 如果所有桶exp时间过期，则统计一次，删除对应key
				if now.After(v.expiration) {
					atomic.AddUint64(&count, 1)
					tbm.deleteValue(k, v)
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	if tbm.fnOnCleaned != nil {
		tbm.fnOnCleaned(time.Now().Sub(start), atomic.LoadUint64(&count), uint64(tbm.Len()))
	}
}

// deleteValue do callback function & delete the expired key
func (tbm *TimeBoundMap) deleteValue(extK extKey, extV *extValue) {
	if extV.cb != nil {
		extV.cb(extK.key, extV.value)
	}

	// put value to Pool
	tbm.valuePool.Put(extV)
	// delete buckets date By extKey
	delete(tbm.buckets[extK.bucketIdx], extK)
}
