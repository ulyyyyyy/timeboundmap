package timeboundmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type TimeBoundMap interface {
	Set(key, value any, lifetime time.Duration, onCleaned ...CallbackFunc)
	Get(key any) (value any, ok bool)

	GetToDoWithLock(key any, do func(value any, ok bool))
	UnsafeSet(key, value any, lifetime time.Duration, onCleaned ...CallbackFunc)
	UnsafeGet(key any) (value any, ok bool)

	Len() int
	Snapshot() map[any]any
}

type CallbackFunc func(key, value any)

type extValue struct {
	val        any
	expiration time.Time
	cb         CallbackFunc
}

func newExtValue(val any, expiration time.Time, cb CallbackFunc) *extValue {
	return &extValue{
		val:        val,
		expiration: expiration,
		cb:         cb, // onCleaned function
	}
}

type extKey struct {
	val any
}

// Bytes Convert key to Bytes
func (k extKey) Bytes() []byte {
	switch v := k.val.(type) {
	case string:
		return []byte(v)
	case *bool, bool, []bool,
		*int8, int8, []int8,
		*uint8, uint8, []uint8,
		*int16, int16, []int16,
		*uint16, uint16, []uint16,
		*int32, int32, []int32,
		*uint32, uint32, []uint32,
		*int64, int64, []int64,
		*uint64, uint64, []uint64,
		*float32, float32, []float32,
		*float64, float64, []float64:

		buf := new(bytes.Buffer)
		// err is always nil
		_ = binary.Write(buf, binary.LittleEndian, v)
		return buf.Bytes()
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}
