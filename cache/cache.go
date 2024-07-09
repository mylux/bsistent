package cache

import (
	"github.com/mylux/bsistent/interfaces"

	xmaps "golang.org/x/exp/maps"
)

type Cache[DataType any] struct {
	pageGen     func() interfaces.Page[DataType]
	pagePool    []interfaces.Page[DataType]
	cache       map[int64]uint32
	invalidated map[uint32]bool
	limit       uint32
	index       uint32
}

type Config[DataType any] struct {
	Limit         uint32
	PageGenerator func() interfaces.Page[DataType]
}

func (c *Cache[DataType]) Save(pg interfaces.Page[DataType], updateOnly ...bool) {
	if c.limit > 0 {
		offset := pg.Offset()
		index := c.nextIndex()
		if locationInPool, exists := c.cache[offset]; exists {
			c.pagePool[locationInPool] = pg
		} else if len(updateOnly) == 0 || !updateOnly[0] {
			invOffset := c.pagePool[index].Offset()
			if invOffset > 0 {
				delete(c.cache, invOffset)
			}
			c.pagePool[index] = pg
			c.cache[offset] = index
		}
	}
}

func (c *Cache[DataType]) Invalidate(offset int64) {
	if c.limit > 0 {
		if locationInPool, exists := c.cache[offset]; exists {
			c.pagePool[locationInPool] = c.pageGen()
			c.invalidated[locationInPool] = true
			delete(c.cache, offset)
			if len(c.cache) == 0 {
				xmaps.Clear(c.invalidated)
				c.index = 0
			}
		}
	}
}

func (c *Cache[DataType]) Load(offset int64) interfaces.Page[DataType] {
	if c.limit > 0 {
		if locationInPool, exists := c.cache[offset]; exists {
			return c.pagePool[locationInPool]
		}
	}
	return nil
}

func (c *Cache[DataType]) Update(pg interfaces.Page[DataType]) {
	c.Save(pg, true)
}

func New[DataType any](config *Config[DataType]) *Cache[DataType] {
	return &Cache[DataType]{
		limit:       config.Limit,
		pageGen:     config.PageGenerator,
		pagePool:    generatePagePool[DataType](config.Limit, config.PageGenerator),
		cache:       make(map[int64]uint32, config.Limit),
		invalidated: make(map[uint32]bool, config.Limit),
	}
}

func generatePagePool[DataType any](limit uint32, pageGen func() interfaces.Page[DataType]) []interfaces.Page[DataType] {
	r := make([]interfaces.Page[DataType], limit)
	for i := range limit {
		r[i] = pageGen()
	}
	return r
}

func (c *Cache[DataType]) nextIndex() uint32 {
	if len(c.cache) == 0 {
		return 0
	}
	if len(c.invalidated) > 0 {
		i := xmaps.Keys(c.invalidated)[0]
		delete(c.invalidated, i)
		return i
	}
	c.index = (c.index + 1) % c.limit
	return c.index

}
