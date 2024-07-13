package btree

import (
	"slices"

	"github.com/mylux/bsistent/interfaces"
)

type BTChildPage[DataType any] struct {
	Offset int64
	Page   interfaces.Page[DataType]
}

type BTPageChildren[DataType any] struct {
	children []*BTChildPage[DataType]
}

func (b *BTPageChildren[DataType]) All() []interfaces.Page[DataType] {
	r := make([]interfaces.Page[DataType], b.Size())
	for i := range b.Size() {
		r[i] = b.children[i].Page
	}
	return r
}

func (b *BTPageChildren[DataType]) ChildFor(item interfaces.Item[DataType]) interfaces.Page[DataType] {
	if len(b.children) == 0 {
		return nil
	}
	return b.Nth(b.children[0].Page.Parent().Items().SlotFor(item))
}

func (b *BTPageChildren[DataType]) Insert(child interfaces.Page[DataType], slot int) {
	b.children = slices.Insert(b.children, slot, b.createChildPage(child))
}

func (b *BTPageChildren[DataType]) IsFetched() bool {
	if b.Size() == 0 {
		return true
	}
	return b.children[0].Page != nil
}

func (b *BTPageChildren[DataType]) Last() interfaces.Page[DataType] {
	return b.Nth(b.Size() - 1)
}

func (b *BTPageChildren[DataType]) Nth(n int) interfaces.Page[DataType] {
	if n >= b.Size() {
		return nil
	}
	return b.children[n].Page
}

func (b *BTPageChildren[DataType]) Offsets() []int64 {
	r := make([]int64, b.Size())
	for i, c := range b.children {
		r[i] = c.Offset
	}
	return r
}

func (b *BTPageChildren[DataType]) Put(offset int64) {
	b.children = append(b.children, &BTChildPage[DataType]{Offset: offset})
}

func (b *BTPageChildren[DataType]) Set(parent interfaces.Page[DataType], pages ...interfaces.Page[DataType]) interfaces.PageChildren[DataType] {
	b.children = make([]*BTChildPage[DataType], len(pages))
	for i, p := range pages {
		p.Parent(parent)
		b.children[i] = b.createChildPage(p)
	}
	return b
}

func (b *BTPageChildren[DataType]) Size() int {
	return len(b.children)
}

func (b *BTPageChildren[DataType]) createChildPage(page interfaces.Page[DataType]) *BTChildPage[DataType] {
	return &BTChildPage[DataType]{Offset: page.Offset(), Page: page}
}
