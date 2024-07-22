package serialization_test

import (
	"testing"
	"unsafe"

	"golang.org/x/exp/maps"

	"github.com/mylux/bsistent/serialization"
	"github.com/stretchr/testify/assert"
)

type attribute struct {
	Name  string
	Value string
}

type mytest struct {
	Id   int
	Name string
}

type mynestedtest struct {
	Id   int
	Name string
	Test mytest
}

type myslicedtest struct {
	Id         int
	Name       string
	Numbers    []int
	Nested     []mytest
	Attributes map[string]attribute
}

type mysecondtype struct {
	Id int64
}

type mytesttype struct {
	Id         int64
	Other      mysecondtype
	Name       string
	Numbers    []int64
	Attributes map[string]*attribute
}

func TestByteWriterWrite(t *testing.T) {
	x := mytest{
		Id:   1,
		Name: "Test",
	}
	w := &serialization.Serializer{}
	result, err := w.Serialize(x)
	assert.Nil(t, err)
	assert.Greater(t, len(result), 0)
}

func TestByteWriterWriteNested(t *testing.T) {
	x := mynestedtest{
		Id:   12,
		Name: "Nested",
		Test: mytest{
			Id:   1,
			Name: "Test",
		},
	}
	w := serialization.Serializer{}
	result, err := w.Serialize(x)
	assert.Nil(t, err)
	assert.Greater(t, len(result), 0)
}

func TestByteWriterWriteSlice(t *testing.T) {
	x := myslicedtest{
		Id:      13,
		Name:    "Sliced",
		Numbers: []int{1, 2, 3},
		Nested: []mytest{
			{Id: 131, Name: "Test1"},
			{Id: 132, Name: "Test2"},
		},
		Attributes: map[string]attribute{
			"name": {Name: "Name", Value: "John Doe"},
			"age":  {Name: "Age", Value: "61 years old"},
		},
	}
	w := serialization.Serializer{}
	result, err := w.Serialize(x)
	assert.Nil(t, err)
	assert.Greater(t, len(result), 0)
}

func TestByteWriterRead(t *testing.T) {
	var x2 mytest
	x1 := mytest{
		Id:   1,
		Name: "Test",
	}
	w := serialization.Serializer{}
	result, err := w.Serialize(x1)
	assert.Nil(t, err)
	err = w.Deserialize(result, &x2)
	assert.Nil(t, err)
	assert.Equal(t, x1, x2)
}

func TestByteWriterReadNested(t *testing.T) {
	var x2 mynestedtest
	x1 := mynestedtest{
		Id:   12,
		Name: "Nested",
		Test: mytest{
			Id:   1,
			Name: "Test",
		},
	}
	w := serialization.Serializer{}
	result, err := w.Serialize(x1)
	assert.Nil(t, err)
	assert.Greater(t, len(result), 0)
	err = w.Deserialize(result, &x2)
	assert.Nil(t, err)
	assert.Equal(t, x1, x2)
}

func TestByteWriterReadSlice(t *testing.T) {
	var x2 myslicedtest
	x1 := myslicedtest{
		Id:      13,
		Name:    "Sliced",
		Numbers: []int{1, 2, 3},
		Nested: []mytest{
			{Id: 131, Name: "Test1"},
			{Id: 132, Name: "Test2"},
		},
		Attributes: map[string]attribute{
			"name": {Name: "Name", Value: "John Doe"},
			"age":  {Name: "Age", Value: "61 years old"},
		},
	}
	w := serialization.Serializer{}
	result, err := w.Serialize(x1)
	assert.Nil(t, err)
	assert.Greater(t, len(result), 0)
	err = w.Deserialize(result, &x2)
	assert.Nil(t, err)
	assert.Equal(t, x1, x2)
}

func TestSizeOf(t *testing.T) {
	x := mytesttype{
		Id: 1,
		Other: mysecondtype{
			Id: 0,
		},
		Name:    "MyTest99",
		Numbers: []int64{12, 24, 36},
		Attributes: map[string]*attribute{
			"attr1": {
				Name:  "Attribute1",
				Value: "Value1",
			},
			"attr2": {
				Name:  "Attribute2",
				Value: "Value2",
			},
		},
	}
	sizeBytes := 4
	mySecondTypeSize := sizeBytes + int(unsafe.Sizeof(x.Other.Id))
	nameSize := sizeBytes + len(x.Name)
	numbersSize := sizeBytes + (len(x.Numbers) * int(unsafe.Sizeof(x.Numbers[0])))
	attributeSize := sizeBytes + sizeBytes + len(x.Attributes["attr1"].Name) + sizeBytes + len(x.Attributes["attr1"].Value)
	attributeKeySize := sizeBytes + len(maps.Keys(x.Attributes)[0])
	attributesSize := sizeBytes + (len(x.Attributes) * (attributeKeySize + attributeSize))

	var n int = sizeBytes + int(unsafe.Sizeof(x.Id)) + mySecondTypeSize + nameSize + numbersSize + attributesSize
	s, err := (&serialization.Serializer{}).SizeOf(x)
	assert.Nil(t, err)
	assert.Equal(t, n, s)
}
