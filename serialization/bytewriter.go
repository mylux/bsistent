package serialization

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

type serializerFunc func(reflect.Value, ...*bytes.Buffer) ([]byte, error)

func (b *Serializer) Serialize(data interface{}) ([]byte, error) {
	var err error
	var sf serializerFunc
	val := reflect.ValueOf(data)
	typ := val.Type()

	if typ.Kind() == reflect.Ptr {
		val = val.Elem()
		typ = val.Type()
	}

	if sf, err = b.getSerializerFunc(typ.Kind()); err == nil {
		return sf(val)
	} else {
		return nil, err
	}
}

func (b *Serializer) getSerializerFunc(kind reflect.Kind) (serializerFunc, error) {
	if b.isFixedSizeType(kind) {
		return b.serializeFixedSize, nil
	} else if kind == reflect.String {
		return b.serializeString, nil
	} else if kind == reflect.Struct {
		return b.serializeStruct, nil
	} else if kind == reflect.Slice {
		return b.serializeSlice, nil
	} else if kind == reflect.Map {
		return b.serializeMap, nil
	} else {
		return nil, fmt.Errorf("unsupported field type for serialization: %s", kind)
	}
}

func (b *Serializer) serializeFixedSize(val reflect.Value, buff ...*bytes.Buffer) ([]byte, error) {
	buf := b.getBuffer(buff)
	if val.Kind() == reflect.Int {
		val = reflect.ValueOf(val.Int())
	}
	if err := binary.Write(buf, binary.LittleEndian, val.Interface()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Serializer) serializeStruct(val reflect.Value, buff ...*bytes.Buffer) ([]byte, error) {
	var err error
	var sf serializerFunc

	buf := b.getBuffer(buff)
	tempBuf := new(bytes.Buffer)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				continue
			}
			field = field.Elem()
		}

		if sf, err = b.getSerializerFunc(field.Kind()); err != nil {
			return nil, err
		}
		if _, err = sf(field, tempBuf); err != nil {
			return nil, err
		}
	}
	structLen := int32(len(tempBuf.Bytes()))
	if err := binary.Write(buf, binary.LittleEndian, structLen); err != nil {
		return nil, err
	}
	buf.Write(tempBuf.Bytes())
	return buf.Bytes(), nil
}

func (b *Serializer) serializeString(field reflect.Value, buff ...*bytes.Buffer) ([]byte, error) {
	buf := b.getBuffer(buff)
	strBytes := []byte(field.String())
	strLen := int32(len(strBytes))
	if err := binary.Write(buf, binary.LittleEndian, strLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, strBytes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Serializer) serializeSlice(field reflect.Value, buff ...*bytes.Buffer) ([]byte, error) {
	buf := b.getBuffer(buff)
	sliceLen := int32(field.Len())
	if err := binary.Write(buf, binary.LittleEndian, sliceLen); err != nil {
		return nil, err
	}
	for j := 0; j < field.Len(); j++ {
		elem := field.Index(j)
		elemBytes, err := b.Serialize(elem.Interface())
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, elemBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (b *Serializer) serializeMap(field reflect.Value, buff ...*bytes.Buffer) ([]byte, error) {
	buf := b.getBuffer(buff)

	mapLen := int32(field.Len())

	if err := binary.Write(buf, binary.LittleEndian, mapLen); err != nil {
		return nil, err
	}
	for _, key := range field.MapKeys() {
		keyBytes, err := b.Serialize(key.Interface())
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}

		value := field.MapIndex(key)
		valueBytes, err := b.Serialize(value.Interface())
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, valueBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (b *Serializer) getBuffer(buff []*bytes.Buffer) *bytes.Buffer {
	if len(buff) > 0 && buff[0] != nil {
		return buff[0]
	} else {
		return new(bytes.Buffer)
	}
}
