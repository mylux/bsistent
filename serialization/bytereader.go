package serialization

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

type deserializeFunc func(*bytes.Reader, reflect.Value) error

func (b *Serializer) Deserialize(data []byte, result interface{}) error {
	var err error
	var df deserializeFunc
	val := reflect.ValueOf(result).Elem()
	typ := val.Type()
	buf := bytes.NewReader(data)

	if df, err = b.getDeserializerFunc(typ.Kind()); err != nil {
		return err
	}
	return df(buf, val)
}

func (b *Serializer) getDeserializerFunc(kind reflect.Kind) (deserializeFunc, error) {
	if b.isFixedSizeType(kind) {
		return b.deserializeFixed, nil
	} else if kind == reflect.String {
		return b.deserializeString, nil
	} else if kind == reflect.Struct {
		return b.deserializeStruct, nil
	} else if kind == reflect.Slice {
		return b.deserializeSlice, nil
	} else if kind == reflect.Array {
		return b.deserializeArray, nil
	} else if kind == reflect.Map {
		return b.deserializeMap, nil
	} else {
		return nil, fmt.Errorf("unsupported field type for deserialization: %s", kind)
	}
}

func (b *Serializer) deserializeStruct(buf *bytes.Reader, val reflect.Value) error {
	var df deserializeFunc
	var err error
	var structLen int32
	if err := binary.Read(buf, binary.LittleEndian, &structLen); err != nil {
		return err
	}
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				continue
			}
			field = field.Elem()
		}
		if df, err = b.getDeserializerFunc(field.Kind()); err != nil {
			return err
		}
		if err = df(buf, field); err != nil {
			return err
		}
	}
	return nil
}

func (b *Serializer) deserializeInt(buf *bytes.Reader, field reflect.Value) error {
	var tempInt int64
	err := binary.Read(buf, binary.LittleEndian, &tempInt)
	field.SetInt(tempInt)
	return err
}

func (b *Serializer) deserializeFixed(buf *bytes.Reader, field reflect.Value) error {
	if field.Kind() == reflect.Int {
		var err error
		if err = b.deserializeInt(buf, field); err != nil {
			return fmt.Errorf("error deserializing int type in struct: %s", err)
		}
		return err
	}
	return binary.Read(buf, binary.LittleEndian, field.Addr().Interface())
}

func (b *Serializer) deserializeString(buf *bytes.Reader, field reflect.Value) error {
	var strLen int32
	if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
		return fmt.Errorf("error deserializing string size in struct: %s", err)
	}
	strBytes := make([]byte, strLen)
	if err := binary.Read(buf, binary.LittleEndian, strBytes); err != nil {
		return fmt.Errorf("error deserializing string content (size = %d) in struct: %s", strLen, err)
	}
	field.SetString(string(strBytes))
	return nil
}

func (b *Serializer) deserializeSlice(buf *bytes.Reader, field reflect.Value) error {
	var sliceLen int32
	var df deserializeFunc
	var err error
	if err = binary.Read(buf, binary.LittleEndian, &sliceLen); err != nil {
		return fmt.Errorf("error deserializing slice size: %s", err)
	}
	sliceType := field.Type().Elem()
	slice := reflect.MakeSlice(field.Type(), int(sliceLen), int(sliceLen))
	for j := 0; j < int(sliceLen); j++ {
		elem := reflect.New(sliceType).Elem()
		if df, err = b.getDeserializerFunc(elem.Kind()); err != nil {
			return err
		}
		if err = df(buf, elem); err != nil {
			return err
		}
		slice.Index(j).Set(elem)
	}
	field.Set(slice)
	return nil
}

func (b *Serializer) deserializeArray(buf *bytes.Reader, field reflect.Value) error {
	arrayLen := field.Len()
	arrayType := field.Type().Elem()
	for j := 0; j < arrayLen; j++ {
		elem := field.Index(j)
		elemBytes := make([]byte, arrayType.Size())
		if err := binary.Read(buf, binary.LittleEndian, elemBytes); err != nil {
			return err
		}
		if err := b.Deserialize(elemBytes, elem.Addr().Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (b *Serializer) deserializeMap(buf *bytes.Reader, field reflect.Value) error {
	var mapLen int32
	var df deserializeFunc
	var err error

	if err = binary.Read(buf, binary.LittleEndian, &mapLen); err != nil {
		return err
	}
	mapType := field.Type()
	mapValue := reflect.MakeMap(mapType)
	keyType := mapType.Key()
	elemType := mapType.Elem()
	for j := 0; j < int(mapLen); j++ {
		key := reflect.New(keyType).Elem()
		if df, err = b.getDeserializerFunc(key.Kind()); err != nil {
			return err
		}
		if err = df(buf, key); err != nil {
			return err
		}
		value := reflect.New(elemType).Elem()
		if df, err = b.getDeserializerFunc(value.Kind()); err != nil {
			return err
		}
		if err = df(buf, value); err != nil {
			return err
		}
		mapValue.SetMapIndex(key, value)
	}
	field.Set(mapValue)
	return nil
}
