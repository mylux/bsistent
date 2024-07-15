package serialization_test

import (
	"testing"

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
