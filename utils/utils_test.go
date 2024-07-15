package utils_test

import (
	"testing"
	"unsafe"

	"golang.org/x/exp/maps"

	"github.com/mylux/bsistent/utils"
	"github.com/stretchr/testify/assert"
)

type attribute struct {
	Name  string
	Value string
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
	s, err := utils.SizeOf(x)
	assert.Nil(t, err)
	assert.Equal(t, n, s)
}
