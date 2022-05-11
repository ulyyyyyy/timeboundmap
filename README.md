# timeboundmap

timeboundmap is safe for concurrent user by multiple goroutines without additional locking or coordination.

it can be used for timed cleaning the key which you want to ignore, like TTL.


## Benchmark

```text
goos: darwin
goarch: amd64
pkg: github.com/ulyyyyyy/timeboundmap
cpu: Intel(R) Core(TM) i5-8259U CPU @ 2.30GHz

BenchmarkTimeBoundMap_Set-8   	 1000000	      1444 ns/op
BenchmarkTimeBoundMap_Get-8   	 1522558	      1130 ns/op
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

```

Please open the test package, if u need to learn more information. 