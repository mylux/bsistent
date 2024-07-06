package btree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"reflect"

	"github.com/mylux/bsistent/interfaces"
)

type BTItem[DataType any] struct {
	content  DataType
	capacity int64
}

func (b *BTItem[DataType]) Capacity() int64 {
	return b.capacity
}

func (b *BTItem[DataType]) Compare(j interfaces.Item[DataType]) (int, error) {
	c, err := getBytes(b.content)
	if err != nil {
		return -2, err
	}
	d, err := getBytes(j.Content())
	if err != nil {
		return -3, err
	}
	return new(big.Int).SetBytes(c).Cmp(new(big.Int).SetBytes(d)), nil
}

func (b *BTItem[DataType]) Content() DataType {
	return b.content
}

func (b *BTItem[DataType]) IsEmpty() bool {
	return reflect.DeepEqual(b.content, reflect.Zero(reflect.TypeOf(b.content)).Interface())
}

func (b *BTItem[DataType]) Load(value DataType) interfaces.Item[DataType] {
	v, err := readValueSize(value)
	if err != nil {
		return nil
	}
	if b.capacity >= int64(v) {
		b.content = value
	}
	return b
}

func (b *BTItem[DataType]) String() string {
	return fmt.Sprintf("{%v}", b.content)
}

func getBytes(s any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readValueSize(s any) (int, error) {
	v, err := getBytes(s)
	if err != nil {
		return -1, err
	}
	return len(v), nil
}

func item[DataType any](capacity int64) interfaces.Item[DataType] {
	return &BTItem[DataType]{capacity: capacity}
}
