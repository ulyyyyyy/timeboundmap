package timeboundmap

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTimeBoundMap(t *testing.T) {
	tbm := New(1*time.Second,
		WithSegmentSize(5),
		WithOnClearingUp(func(elapsed time.Duration, removed, remaining uint64) {
			fmt.Printf("elapsed time: %v, removed %d keys and remaining %d keys\n", elapsed, removed, remaining)
		}),
	)

	tbm.Set(1, "one", 2*time.Second, func(key, value any) {
		fmt.Printf("key: %v has been removed. The value is %v\n", key, value)
	})

	tbm.Get(1)
	fmt.Println(tbm.Snapshot())
	time.Sleep(2500 * time.Millisecond)

	tbm.Get(1)
	fmt.Println(tbm.Snapshot())
}

func BenchmarkTimeBoundMap_Set(b *testing.B) {
	b.StopTimer()

	tbm := New(30 * time.Minute)
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

	tbm := New(30 * time.Minute)
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

	// tbm := New(30 * time.Minute)
	tbm := New(30*time.Minute, WithSegmentSize(16*12))
	// tbm := New(30*time.Minute, WithSegmentSize(16*12*10))

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
			tbm.GetToDoWithLock(key, func(value any, ok bool) {
				if !ok {
					cv := testData[key] + 1
					tbm.UnsafeSet(key, &cv, 10*time.Minute)
				} else {
					cv := value.(*int64)
					atomic.AddInt64(cv, 1)
				}
			})
		}(key)
	}

	wg.Wait()

	b.StopTimer()

	var count int64
	for k, v := range tbm.Snapshot() {
		testVal := testData[k.(string)]
		count += atomic.LoadInt64(v.(*int64)) - testVal
	}
	if count != int64(b.N) {
		b.FailNow()
	}

	// goos: darwin
	// goarch: amd64
	// pkg: github.com/ulyyyyyy/timeboundmap
	// cpu: Intel(R) Core(TM) i5-8259U CPU @ 2.30GHz
	// BenchmarkCounter
	// BenchmarkCounter-8   	 2316027	       742.3 ns/op
}
