package btree

import (
	"fmt"
	"os"

	"github.com/mylux/bsistent/assemblers"
	"github.com/mylux/bsistent/interfaces"
)

var defaultConfig BTConfig[any] = BTConfig[any]{
	grade:       500,
	itemSize:    64,
	size:        0,
	storagePath: fmt.Sprintf("%s/.bsistent/bsistent", os.Getenv("HOME")),
	reset:       false,
	cacheSize:   0,
}

type BTConfig[DataType any] struct {
	grade       int
	itemSize    int64
	size        int64
	storagePath string
	reset       bool
	cacheSize   uint32
}

func Configuration[DataType any]() *BTConfig[DataType] {
	return &BTConfig[DataType]{
		grade:       defaultConfig.grade,
		itemSize:    defaultConfig.itemSize,
		storagePath: defaultConfig.storagePath,
		reset:       defaultConfig.reset,
	}
}

func (c *BTConfig[DataType]) Reset() *BTConfig[DataType] {
	c.reset = !c.reset
	return c
}

func (c *BTConfig[DataType]) Grade(grade int) *BTConfig[DataType] {
	c.grade = grade
	return c
}

func (c *BTConfig[DataType]) ItemSize(itemSize int64) *BTConfig[DataType] {
	c.itemSize = itemSize
	return c
}

func (c *BTConfig[DataType]) StoragePath(storagePath string) *BTConfig[DataType] {
	c.storagePath = storagePath
	return c
}

func (c *BTConfig[DataType]) CacheSize(size uint32) *BTConfig[DataType] {
	c.cacheSize = size
	return c
}

func (c *BTConfig[DataType]) Make() *Btree[DataType] {
	fp := func(offset int64) interfaces.Page[DataType] {
		return page[DataType](offset, c.grade-1)
	}

	fi := func() interfaces.Item[DataType] {
		return item[DataType](c.itemSize)
	}
	p := assemblers.NewPersistence[DataType](
		&interfaces.PersistenceConfig[DataType]{
			Path:            c.storagePath,
			PageConstructor: fp,
			ItemConstructor: fi,
			CacheSize:       c.cacheSize,
		})
	return btree[DataType](c.grade, c.itemSize, c.storagePath, c.reset, p)
}
