package timeboundmap

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDemoNewTimeBoundMap(t *testing.T) {
	var wg = sync.WaitGroup{}

	boundMap := New[string, int](1*time.Second, WithSegmentSize(4), WithOnClearingUp(func(elapsed time.Duration, removed, remaining uint64) {
		t.Log("elapsed time:", elapsed)
		t.Log("the number of removing keys", removed)
		t.Log("the number of remaining keys", remaining)
		t.Log(time.Now().Format("2006-01-02 15:04:05"))
	}))
	wg.Add(2)

	boundMap.Set("key1", 1, 2*time.Second, func(key, value interface{}) {
		t.Log("[WARN] key1 elapsed, the value is", value)
		wg.Done()
	})
	boundMap.Set("key2", 2, 4*time.Second, func(key, value interface{}) {
		t.Log("[WARN] key2 elapsed, too. The value is", value)
		wg.Done()
	})

	wg.Wait()
	t.Log("Demo end.")
}

func BenchmarkTimeBoundMap_Set(b *testing.B) {
	b.StopTimer()

	tbm := New[string, uint64](30 * time.Minute)
	lifetime := time.Minute

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		tbm.Set(randStr(), r.Uint64(), lifetime)
	}

	b.StopTimer()

	if tbm.Len() != b.N {
		b.FailNow()
	}
}

func BenchmarkTimeBoundMap_Get(b *testing.B) {
	b.StopTimer()

	tbm := New[string, uint64](30 * time.Minute)
	lifetime := time.Minute
	keys := make([]string, b.N)
	for i := range keys {
		k := randStr()
		v := r.Uint64()
		keys[i] = k
		tbm.Set(k, v, lifetime)
	}
	fnRandKey := func() string {
		return keys[r.Intn(len(keys))]
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		tbm.Get(fnRandKey())
	}

	b.StopTimer()

	if tbm.Len() != b.N {
		b.FailNow()
	}
}

var r = rand.New(rand.NewSource(time.Now().Unix()))

func randStr() string {
	bs := make([]byte, 16)
	r.Read(bs)
	return fmt.Sprintf("%x", bs)
}

func BenchmarkCounter(b *testing.B) {
	b.StopTimer()

	tbm := New[string, int64](30*time.Minute, WithSegmentSize(16*12))

	testData := make(map[string]int64, b.N)
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = randStr()
		testData[keys[i]] = r.Int63n(int64(i) + 100)
	}
	fnRandKey := func() string {
		return keys[r.Intn(len(keys))]
	}

	var wg sync.WaitGroup
	wg.Add(b.N)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		key := fnRandKey()
		go func(key string) {
			defer wg.Done()
			tbm.GetToDoWithLock(key, func(value int64, ok bool) {
				if !ok {
					cv := testData[key] + 1
					tbm.UnsafeSet(key, cv, 10*time.Minute)
				} else {
					cv := value
					atomic.AddInt64(&cv, 1)
				}
			})
		}(key)
	}

	wg.Wait()

	b.StopTimer()

	var count int64
	for k, v := range tbm.Snapshot() {
		testVal := testData[k]
		count += atomic.LoadInt64(&v) - testVal
	}

	// goos: windows
	// goarch: amd64
	// pkg: github.com/ulyyyyyy/timeboundmap
	// cpu: Intel(R) Core(TM) i7-9700 CPU @ 3.00GHz
	// BenchmarkCounter
	// BenchmarkCounter-8       2885394               468.8 ns/op
}
