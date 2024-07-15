package btree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"math/big"
	"reflect"

	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
)

type BTItem[DataType any] struct {
	content  DataType
	capacity int64
}

func (b *BTItem[DataType]) Capacity() int64 {
	return b.capacity
}

func (b *BTItem[DataType]) Compare(j interfaces.Item[DataType]) (int, error) {
	var r int
	compareFieldsB := utils.GetTaggedFieldValues(b.Content(), bsistentTagName, bsistentTagKeyName)
	compareFieldsJ := utils.GetTaggedFieldValues(j.Content(), bsistentTagName, bsistentTagKeyName)
	if lcf := len(compareFieldsB); lcf == 0 || lcf != len(compareFieldsJ) {
		return b.compareBytes(b.content, j.Content())
	}
	for i := range compareFieldsB {
		c, err := b.compareBytes(compareFieldsB[i], compareFieldsJ[i])
		if err != nil {
			return -2, err
		}
		r += c * int(math.Pow(2, float64(i)))
	}
	if r > 0 {
		return 1, nil
	} else if r == 0 {
		return 0, nil
	}
	return -1, nil
}

func (b *BTItem[DataType]) Content() DataType {
	return b.content
}

func (b *BTItem[DataType]) IsEmpty() bool {
	return reflect.DeepEqual(b.content, reflect.Zero(reflect.TypeOf(b.content)).Interface())
}

func (b *BTItem[DataType]) Load(value DataType) interfaces.Item[DataType] {
	v, err := utils.SizeOf(value)
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

func (b *BTItem[DataType]) compareBytes(p any, q any) (int, error) {
	c, err := getBytes(p)
	if err != nil {
		return -2, err
	}
	d, err := getBytes(q)
	if err != nil {
		return -3, err
	}
	return new(big.Int).SetBytes(c).Cmp(new(big.Int).SetBytes(d)), nil
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

func item[DataType any](capacity int64) interfaces.Item[DataType] {
	return &BTItem[DataType]{capacity: capacity}
}
