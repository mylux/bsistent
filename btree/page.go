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
	children []*BTChildPage[DataType]
}

type BTChildPage[DataType any] struct {
	Offset int64
	Page   interfaces.Page[DataType]
}

func page[DataType any](offset int64, capacity int) interfaces.Page[DataType] {
	return &BTPage[DataType]{
		capacity: capacity,
		items:    make([]interfaces.Item[DataType], 0, capacity+1),
		children: make([]*BTChildPage[DataType], 0, capacity+2),
		offset:   offset,
	}
}

func (b *BTPage[DataType]) Add(item interfaces.Item[DataType]) {
	if !b.IsFull() {
		b.items = slices.Insert(b.items, b.Items().SlotFor(item), item)
	}
}

func (b *BTPage[DataType]) AddChild(child interfaces.Page[DataType]) {
	var slot int = 0
	greatestItem := child.Items().Last()
	if len(b.children) > 0 {
		slot = b.Items().SlotFor(greatestItem)
	}
	b.children = slices.Insert(b.children, slot, b.createChildPage(child))
	child.Parent(b)
}

func (b *BTPage[DataType]) AppendChildOffset(offset int64) {
	b.children = append(b.children, &BTChildPage[DataType]{Offset: offset})
}

func (b *BTPage[DataType]) Capacity() int {
	return b.capacity
}

func (b *BTPage[DataType]) ChildFor(item interfaces.Item[DataType]) interfaces.Page[DataType] {
	if b.IsLeaf() {
		return nil
	}
	return b.children[b.Items().SlotFor(item)].Page
}

func (b *BTPage[DataType]) Child(index int) interfaces.Page[DataType] {
	if index >= len(b.children) {
		return nil
	}
	return b.children[index].Page
}

func (b *BTPage[DataType]) Children(children ...[]interfaces.Page[DataType]) []interfaces.Page[DataType] {
	if len(children) > 0 {
		b.children = b.pageListToChildren(children[0], true)
	}
	return b.childrenToPageList(b.children)
}

func (b *BTPage[DataType]) IsLeaf() bool {
	return len(b.children) == 0
}

func (b *BTPage[DataType]) GetChildrenStatus() (bool, []int64) {
	offsets := make([]int64, len(b.children))
	for i, c := range b.children {
		offsets[i] = c.Offset
	}
	return !b.IsLeaf() && b.children[0].Page != nil, offsets
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

func (p *BTPage[DataType]) UnloadChildren() {
	for _, c := range p.children {
		c.Page = nil
	}
}

func (p *BTPage[DataType]) createChildPage(page interfaces.Page[DataType]) *BTChildPage[DataType] {
	return &BTChildPage[DataType]{Offset: page.Offset(), Page: page}
}

func (p *BTPage[DataType]) childrenToPageList(children []*BTChildPage[DataType]) []interfaces.Page[DataType] {
	r := make([]interfaces.Page[DataType], len(children))
	for i, p := range children {
		r[i] = p.Page
	}
	return r
}

func (p *BTPage[DataType]) pageListToChildren(pages []interfaces.Page[DataType], setParent ...bool) []*BTChildPage[DataType] {
	children := make([]*BTChildPage[DataType], 0, len(pages))
	for _, c := range pages {
		if c != nil {
			if len(setParent) > 0 && setParent[0] {
				c.Parent(p)
			}
			children = append(children, p.createChildPage(c))
		}
	}
	return children
}
