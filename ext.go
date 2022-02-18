package timeboundmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type TimeBoundMap interface {
	Set(key, value interface{}, lifetime time.Duration, onCleaned ...CallbackFunc)
	Get(key interface{}) (value interface{}, ok bool)

	GetToDoWithLock(key interface{}, do func(value interface{}, ok bool))
	UnsafeSet(key, value interface{}, lifetime time.Duration, onCleaned ...CallbackFunc)
	UnsafeGet(key interface{}) (value interface{}, ok bool)

	Len() int
	Snapshot() map[interface{}]interface{}
}

type CallbackFunc func(key, value interface{})

type extValue struct {
	val        interface{}
	expiration time.Time
	cb         CallbackFunc
}

func newExtValue(val interface{}, expiration time.Time, cb CallbackFunc) *extValue {
	return &extValue{
		val:        val,
		expiration: expiration,
		cb:         cb,
	}
}

type extKey struct {
	val interface{}
}

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
