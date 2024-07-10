package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/mylux/bsistent/interfaces"
)

type SerializedItem struct {
	Empty   bool
	Content []byte
}

type SerializedPage struct {
	Offset   int64
	Capacity int64
	Items    [][]byte
	Parent   int64
	Children []int64
}

func serializeItem[T any](x interfaces.Item[T]) ([]byte, error) {
	finalValue, err := encode(x.Content())
	if err != nil {
		return nil, err
	}
	return generateEncodedItem(finalValue)
}

func generateEncodedZeroItem(size int64) []byte {
	r, _ := generateEncodedItem(make([]byte, size), true)
	return r
}

func generateEncodedItem(v []byte, empty ...bool) ([]byte, error) {
	var err error
	var e bool = false
	if len(empty) > 0 {
		e = empty[0]
	}
	sb := &bytes.Buffer{}
	r := &SerializedItem{
		Empty:   e,
		Content: v,
	}
	attrsToEncode := []any{r.Empty, r.Content}
	for _, ate := range attrsToEncode {
		_, err = encode(ate, sb)
		if err != nil {
			break
		}
	}
	return sb.Bytes(), err
}

func generateEncodedZeroPage(capacity int, itemValueSize int64) ([]byte, error) {
	encodedItems := make([][]byte, 0, capacity)
	for range capacity {
		encodedItems = append(encodedItems, generateEncodedZeroItem(itemValueSize))
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
	items := make([][]byte, p.Capacity())
	for i := range p.Size() {
		items[i], err = serializeItem[T](p.Item(i))
	}
	for i := p.Size(); i < p.Capacity(); i++ {
		items[i] = generateEncodedZeroItem(p.Item(0).Capacity())
	}
	if err != nil {
		return nil, err
	}

	sChildren := make([]int64, p.Capacity()+1)
	_, children := p.GetChildrenStatus()

	copy(sChildren, children)

	return encodePage(&SerializedPage{
		Offset:   int64(p.Offset()),
		Capacity: int64(p.Capacity()),
		Items:    items,
		Children: sChildren,
	})
}

func encodePage(sp *SerializedPage) ([]byte, error) {
	var err error
	sb := &bytes.Buffer{}
	attrsToEncode := []any{sp.Offset, sp.Capacity, sp.Parent, sp.Items, sp.Children}

	for _, ate := range attrsToEncode {
		_, err = encode(ate, sb)
	}
	return sb.Bytes(), err
}

func hydrateItem(data []byte) (*SerializedItem, error) {
	var err error
	buf := bytes.NewReader(data)
	s := &SerializedItem{}
	attrsToDecode := []any{&s.Empty, &s.Content}

	for _, atd := range attrsToDecode {
		err = decode(buf, atd)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func hydratePage(data []byte) (*SerializedPage, error) {
	var err error
	buf := bytes.NewReader(data)
	sp := &SerializedPage{}
	attributesToDecode := []any{&sp.Offset, &sp.Capacity, &sp.Parent, &sp.Items, &sp.Children}
	for _, atd := range attributesToDecode {
		err = decode(buf, atd)
		if err != nil {
			return nil, err
		}
	}
	return sp, err
}

func decode(r *bytes.Reader, s any) error {
	val := reflect.ValueOf(s).Elem()
	if val.Kind() == reflect.Ptr {
		return fmt.Errorf("decode: argument (%v) must be a pointer, %v found", s, val.Kind())
	}

	if val.Kind() == reflect.Slice {
		var contentLen int32
		if err := decode(r, &contentLen); err != nil {
			return err
		}
		sliceType := val.Type().Elem()
		newSlice := reflect.MakeSlice(val.Type(), int(contentLen), int(contentLen))
		for i := 0; i < int(contentLen); i++ {
			elem := reflect.New(sliceType).Interface()
			if err := decode(r, elem); err != nil {
				return err
			}
			newSlice.Index(i).Set(reflect.ValueOf(elem).Elem())
		}
		val.Set(newSlice)
	} else {
		return binary.Read(r, binary.LittleEndian, s)
	}

	return nil
}

func encode(p any, pbuf ...*bytes.Buffer) ([]byte, error) {
	var buf *bytes.Buffer

	if len(pbuf) > 0 {
		buf = pbuf[0]
	} else {
		buf = &bytes.Buffer{}
	}

	if reflect.TypeOf(p).Kind() == reflect.Slice {
		val := reflect.ValueOf(p)
		err := binary.Write(buf, binary.LittleEndian, int32(val.Len()))
		if err != nil {
			return nil, err
		}

		if val.Type().Elem().Kind() == reflect.Slice {
			for i := 0; i < val.Len(); i++ {
				_, err = encode(val.Index(i).Interface(), buf)
				if err != nil {
					return nil, err
				}
			}
		} else {
			err = binary.Write(buf, binary.LittleEndian, p)
		}
		return buf.Bytes(), err
	} else {
		err := binary.Write(buf, binary.LittleEndian, p)
		return buf.Bytes(), err
	}
}

func resizeSlice(slicePtr *any, newSize int) error {
	if slicePtr == nil {
		return fmt.Errorf("provided slicePtr is nil")
	}
	ptrValue := reflect.ValueOf(slicePtr)
	if ptrValue.Kind() != reflect.Ptr {
		return fmt.Errorf("provided value is not a pointer")
	}
	sliceValue := ptrValue.Elem()
	if !sliceValue.IsValid() || sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("provided value is not a slice")
	}

	newSlice := reflect.MakeSlice(sliceValue.Type(), newSize, newSize)

	for i := 0; i < sliceValue.Len() && i < newSize; i++ {
		newSlice.Index(i).Set(sliceValue.Index(i))
	}

	sliceValue.Set(newSlice)

	return nil
}
