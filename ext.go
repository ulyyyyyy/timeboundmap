package timeboundmap

import "time"

type CallbackFunc func(key, value interface{})

type extKey struct {
	bucketIdx int
	key       interface{}
}

type extValue struct {
	value      interface{}
	expiration time.Time
	cb         CallbackFunc
}
