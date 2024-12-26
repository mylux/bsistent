package utils

import (
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
)

func PanicOnError(f func() error) {
	invokePanicOnError(f())
}

func PanicIf(e bool, message string) {
	if e {
		panic(message)
	}
}

func ErrorIf(e bool, message string) error {
	if e {
		return errors.New(message)
	}
	return nil
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

func GetTaggedFieldValues(v any, tagName, tagKey string, pTagValue ...string) []any {
	r := []any{}
	val := reflect.ValueOf(v)
	typ := reflect.TypeOf(v)
	tagValue := ""
	if len(pTagValue) > 0 {
		tagValue = pTagValue[0]
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		val = val.Elem()
	}
	if typ.Kind() == reflect.Struct {
		for i := 0; i < val.NumField(); i++ {
			found, value := GetFieldTagKey(typ.Field(i), tagName, tagKey)
			if found && value == tagValue {
				r = append(r, val.Field(i).Interface())
				break
			}
		}
	}
	return r
}

func GetFieldTagKey(field reflect.StructField, tagName string, tagKey string) (bool, string) {
	if tagParts := strings.Split(field.Tag.Get(tagName), ";"); len(tagParts) > 0 {
		for _, part := range tagParts {
			if kv := strings.Split(part, ":"); len(kv) > 0 {
				if kv[0] == tagKey {
					switch len(kv) {
					case 1:
						return true, ""
					case 2:
						return true, kv[1]
					default:
						return false, ""
					}
				}
			}
		}
		return false, ""
	}
	return false, ""
}

func CoalesceBool(b []bool) bool {
	return Coalesce(b, false)
}

func Coalesce[T any](v []T, dv T) T {
	if len(v) > 0 {
		return v[0]
	}
	return dv
}

func IntAbs(n int) int {
	return int(math.Abs(float64(n)))
}

func Ternary[T any](cond bool, whenTrue T, whenFalse T) T {
	if cond {
		return whenTrue
	}
	return whenFalse
}

func Range(n int) []int {
	r := make([]int, n)
	for i := 0; i < n; i++ {
		r[i] = i
	}
	return r
}

func GetFirstReturned[T any, U any](f func() (T, U)) T {
	t, _ := f()
	return t
}

func Limit(number int, edges ...int) int {
	if len(edges) == 2 {
		slices.Sort(edges)
		return min(max(edges[0], number), edges[1])
	}
	return number
}
