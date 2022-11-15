# timeboundmap

A Map data structure with expiration cleanup

## Benchmark

```text
goos: darwin
goarch: amd64
pkg: github.com/ulyyyyyy/timeboundmap
cpu: Intel(R) Core(TM) i5-8259U CPU @ 2.30GHz

BenchmarkTimeBoundMap_Set-8   	 1000000	      1444 ns/op
BenchmarkTimeBoundMap_Get-8   	 1522558	      1130 ns/op
```

After use generic

```text
goos: windows
goarch: amd64
pkg: github.com/ulyyyyyy/timeboundmap
cpu: Intel(R) Core(TM) i7-9700 CPU @ 3.00GHz
BenchmarkTimeBoundMap_Set
BenchmarkTimeBoundMap_Set-8      1534119               667.7 ns/op
BenchmarkTimeBoundMap_Get-8      3023389               439.7 ns/op
```

## Example

```go
package timeboundmap

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)


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
```