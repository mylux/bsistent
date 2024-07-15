package utils

import (
	"reflect"

	"github.com/mylux/bsistent/serialization"
)

func PanicOnError(f func() error) {
	invokePanicOnError(f())
}

func ReturnOrPanic[T any](f func() (T, error)) T {
	r, err := f()
	invokePanicOnError(err)
	return r
}

func OnError[T any](f func() (T, error), d T) T {
	r, err := f()
	if err != nil {
		return d
	}
	return r
}

func invokePanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func SizeOf(v interface{}) (int, error) {
	serializer := &serialization.Serializer{}
	s, err := serializer.Serialize(v)
	if err == nil {
		return len(s), err
	}
	return 0, err
}

func GetTaggedFieldValues(v interface{}, tagKey, tagValue string) []any {
	r := []any{}
	val := reflect.ValueOf(v)
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		val = val.Elem()
	}
	if typ.Kind() == reflect.Struct {
		for i := 0; i < val.NumField(); i++ {
			tag := typ.Field(i).Tag.Get(tagKey)
			if tag == tagValue {
				r = append(r, val.Field(i).Interface())
			}
		}
	}
	return r
}
