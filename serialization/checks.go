package serialization

import "reflect"

func (b *Serializer) isFixedSizeType(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool:
		return true
	}
	return false
}

func (b *Serializer) SizeOf(v interface{}) (int, error) {
	serializer := &Serializer{}
	s, err := serializer.Serialize(v)
	if err == nil {
		return len(s), err
	}
	return 0, err
}
