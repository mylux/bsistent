package btree

import (
	"fmt"
	"slices"

	"github.com/mylux/bsistent/interfaces"
)

type BTPage[DataType any] struct {
	offset   int64
	capacity int
	items    []interfaces.Item[DataType]
	parent   interfaces.Page[DataType]
	children *BTPageChildren[DataType]
}

func page[DataType any](offset int64, capacity int) interfaces.Page[DataType] {
	r := &BTPage[DataType]{
		capacity: capacity,
		items:    make([]interfaces.Item[DataType], 0, capacity+1),
		offset:   offset,
		children: &BTPageChildren[DataType]{children: make([]*BTChildPage[DataType], 0, capacity+2)},
	}
	return r
}

func (b *BTPage[DataType]) Add(item interfaces.Item[DataType]) {
	if !b.IsFull() {
		b.items = slices.Insert(b.items, b.Items().SlotFor(item), item)
	}
}

func (b *BTPage[DataType]) AddChild(child interfaces.Page[DataType]) {
	var slot int = 0
	greatestItem := child.Items().Last()
	if b.children.Size() > 0 {
		slot = b.Items().SlotFor(greatestItem)
	}
	b.children.Insert(child, slot)
	child.Parent(b)
}

func (b *BTPage[DataType]) Capacity() int {
	return b.capacity
}

func (b *BTPage[DataType]) Child(index int) interfaces.Page[DataType] {
	if index >= b.children.Size() {
		return nil
	}
	return b.children.Nth(index)
}

func (b *BTPage[DataType]) Children(children ...[]interfaces.Page[DataType]) interfaces.PageChildren[DataType] {
	if len(children) > 0 {
		return b.children.Set(b, children[0]...)
	}
	return b.children
}

func (b *BTPage[DataType]) IsLeaf() bool {
	return b.children.Size() == 0
}

func (b *BTPage[DataType]) Item(index int) interfaces.Item[DataType] {
	if index >= len(b.items) || index < 0 {
		return nil
	}
	return b.items[index]
}

func (p *BTPage[DataType]) Items(items ...interfaces.Item[DataType]) interfaces.PageItems[DataType] {
	if len(items) > 0 && len(items) <= p.capacity {
		p.items = items
	}
	return &BTPageItems[DataType]{page: p}
}

func (b *BTPage[DataType]) IsFull() bool {
	return len(b.items) > b.capacity
}

func (b *BTPage[DataType]) Offset() int64 {
	return b.offset
}

func (b *BTPage[DataType]) Parent(parent ...interfaces.Page[DataType]) interfaces.Page[DataType] {
	if len(parent) > 0 {
		b.parent = parent[0]
	}
	return b.parent
}

func (b *BTPage[DataType]) Size() int {
	return len(b.items)
}

func (p *BTPage[DataType]) String() string {
	s := p.Size()
	if s > 0 {
		return fmt.Sprintf("{%.19d: %v}", p.Offset(), p.Items().ToSlice())
	}
	return "[]"
}
