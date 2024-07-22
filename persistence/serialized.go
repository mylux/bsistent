package persistence

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"

	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/serialization"
)

type SerializedItem struct {
	Empty   bool
	Content []byte
}

type SerializedPage struct {
	Offset   int64
	Capacity int64
	Items    []SerializedItem
	Parent   int64
	Children []int64
}

var serializer *serialization.Serializer = &serialization.Serializer{}

func serializeItem[T any](x interfaces.Item[T]) (*SerializedItem, error) {
	finalValue, err := encode(x.Content())
	if err != nil {
		return nil, err
	}
	cap := int(x.Capacity())
	if size := len(finalValue); size < cap {
		finalValue = slices.Concat(finalValue, make([]byte, cap-size))
	}
	return &SerializedItem{
		Empty:   x.IsEmpty(),
		Content: finalValue,
	}, nil
}

func generateZeroItem(size int64) *SerializedItem {
	return &SerializedItem{
		Empty:   true,
		Content: make([]byte, size),
	}
}

func generateEncodedZeroPage(capacity int, itemValueSize int64) ([]byte, error) {
	encodedItems := make([]SerializedItem, capacity)
	for i := range capacity {
		encodedItems[i] = SerializedItem{
			Empty:   true,
			Content: make([]byte, itemValueSize),
		}
	}

	return encodePage(&SerializedPage{
		Offset:   2,
		Capacity: int64(capacity),
		Parent:   1,
		Items:    encodedItems,
		Children: make([]int64, capacity+1),
	})
}

func serializePage[T any](p interfaces.Page[T]) ([]byte, error) {
	var err error
	var pit *SerializedItem
	items := make([]SerializedItem, p.Capacity())
	for i := range p.Size() {
		pit, err = serializeItem[T](p.Item(i))
		items[i] = *pit
	}
	for i := p.Size(); i < p.Capacity(); i++ {
		items[i] = *generateZeroItem(p.Item(0).Capacity())
	}
	if err != nil {
		return nil, err
	}

	sChildren := make([]int64, p.Capacity()+1)
	children := p.Children().Offsets()

	copy(sChildren, children)

	return encodePage(&SerializedPage{
		Offset:   int64(p.Offset()),
		Capacity: int64(p.Capacity()),
		Items:    items,
		Children: sChildren,
	})
}

func encodePage(sp *SerializedPage) ([]byte, error) {
	return encode(*sp)
}

func hydratePage(data []byte) (*SerializedPage, error) {
	var p SerializedPage
	err := decode(data, &p)
	return &p, err
}

func decode(r []byte, s any) error {
	val := reflect.ValueOf(s).Elem()
	if val.Kind() == reflect.Ptr {
		return fmt.Errorf("decode: argument (%v) must not be a pointer of pointer, %v found", s, val.Kind())
	}
	return serializer.Deserialize(r, s)
}

func encode(p any, pbuf ...*bytes.Buffer) ([]byte, error) {
	var err error
	if s, err := serializer.Serialize(p); err == nil {
		if len(pbuf) > 0 {
			pbuf[0].Write(s)
			return pbuf[0].Bytes(), nil
		}
		return s, err
	}
	return nil, err
}
