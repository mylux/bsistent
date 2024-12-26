package btree

import (
	"math"
	"slices"

	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
	"github.com/samber/lo"
)

type BTChildPage[DataType any] struct {
	Offset int64
	Page   interfaces.Page[DataType]
}

type BTPageChildren[DataType any] struct {
	children []*BTChildPage[DataType]
}

func NewPageChildren[DataType any](parent interfaces.Page[DataType], childrenPages []interfaces.Page[DataType]) interfaces.PageChildren[DataType] {
	children := make([]*BTChildPage[DataType], len(childrenPages))
	for i, child := range childrenPages {
		if parent != nil {
			child.Parent(parent)
		}
		children[i] = &BTChildPage[DataType]{
			Offset: child.Offset(),
			Page:   child,
		}
	}
	return &BTPageChildren[DataType]{
		children: children,
	}
}

func (b *BTPageChildren[DataType]) All() []interfaces.Page[DataType] {
	r := make([]interfaces.Page[DataType], b.Size())
	for i := range b.Size() {
		r[i] = b.children[i].Page
	}
	return r
}

func (b *BTPageChildren[DataType]) BySize(asc ...bool) interfaces.PageChildren[DataType] {
	var r interfaces.PageChildren[DataType]
	if b.Size() > 0 {
		children := b.All()
		slices.SortStableFunc(children, func(c1, c2 interfaces.Page[DataType]) int {
			d := float64(c1.Size() - c2.Size())
			return lo.Ternary(d != 0, int(d/math.Abs(d)), 0)
		})
		if !utils.CoalesceBool(asc) {
			slices.Reverse(children)
		}
		return NewPageChildren(b.Nth(0).Parent(), children)
	}
	return r
}

func (b *BTPageChildren[DataType]) ChildFor(item interfaces.Item[DataType]) interfaces.Page[DataType] {
	if len(b.children) == 0 {
		return nil
	}
	return b.Nth(b.children[0].Page.Parent().Items().SlotFor(item))
}

func (b *BTPageChildren[DataType]) First() interfaces.Page[DataType] {
	return b.Nth(0)
}

func (b *BTPageChildren[DataType]) Insert(child interfaces.Page[DataType], slot int) {
	b.children = slices.Insert(b.children, slot, b.createChildPage(child))
}

func (b *BTPageChildren[DataType]) IsFetched() bool {
	if b.Size() == 0 {
		return true
	}
	return !lo.SomeBy(b.children, func(c *BTChildPage[DataType]) bool { return c.Page == nil })
}

func (b *BTPageChildren[DataType]) Last() interfaces.Page[DataType] {
	return b.Nth(b.Size() - 1)
}

func (b *BTPageChildren[DataType]) LookUp(child interfaces.Page[DataType]) int {
	for i, c := range b.children {
		if child.Offset() == c.Offset {
			return i
		}
	}
	return -1
}

func (b *BTPageChildren[DataType]) Nth(n int) interfaces.Page[DataType] {
	if n >= b.Size() || n < 0 {
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

func (b *BTPageChildren[DataType]) Pick(indexes ...int) interfaces.PageChildren[DataType] {
	cl := make([]interfaces.Page[DataType], 0, len(indexes))
	for _, i := range indexes {
		if c := b.Nth(i); c != nil {
			cl = append(cl, c)
		}
	}
	p := lo.Ternary(len(cl) > 0, cl[0].Parent(), nil)
	return NewPageChildren(p, cl)
}

func (b *BTPageChildren[DataType]) Popped(index int) (int64, interfaces.PageChildren[DataType]) {
	child := b.children[index].Offset
	b.children = slices.Delete(b.children, index, index+1)
	return child, b
}

func (b *BTPageChildren[DataType]) Put(offset int64, index ...int) { // NAO ESTA MANTENDO ORDEM!!!
	nc := &BTChildPage[DataType]{Offset: offset}
	nChildren := len(b.children)
	if where := utils.Coalesce(index, nChildren); where > len(b.children) {
		b.children = append(b.children, nc)
	} else {
		b.children = slices.Insert(b.children, where, nc)
	}
}

func (b *BTPageChildren[DataType]) Set(parent interfaces.Page[DataType], pages ...interfaces.Page[DataType]) interfaces.PageChildren[DataType] {
	b.children = make([]*BTChildPage[DataType], len(pages))
	for i, p := range pages {
		p.Parent(parent)
		b.children[i] = b.createChildPage(p)
	}
	return b
}

func (b *BTPageChildren[DataType]) Siblings(child interfaces.Page[DataType]) interfaces.PageChildren[DataType] {
	index := b.LookUp(child)
	if index == -1 {
		return nil
	}
	var indexes []int
	if index == 0 && b.Size() > 1 {
		indexes = []int{1}
	} else if index == b.Size()-1 {
		indexes = []int{index - 1}
	} else if b.Size() > 1 {
		indexes = []int{index - 1, index + 1}
	}
	return b.Pick(indexes...)
}

func (b *BTPageChildren[DataType]) Size() int {
	return len(b.children)
}

func (b *BTPageChildren[DataType]) Split(middle int) ([]interfaces.Page[DataType], []interfaces.Page[DataType]) {
	var childrenLeft []interfaces.Page[DataType]
	var childrenRight []interfaces.Page[DataType]
	if b.Size() > 0 {
		capacity := b.children[0].Page.Capacity()
		childrenLeft = make([]interfaces.Page[DataType], 0, capacity+2)
		childrenRight = make([]interfaces.Page[DataType], 0, capacity+2)
		for x := 0; x <= middle; x++ {
			childrenLeft = append(childrenLeft, b.Nth(x))
		}
		for x := middle + 1; x <= capacity+1; x++ {
			childrenRight = append(childrenRight, b.Nth(x))
		}
	}
	return childrenLeft, childrenRight
}

func (b *BTPageChildren[DataType]) Unload() {
	for i := range b.children {
		b.children[i].Page = nil
	}
}

func (b *BTPageChildren[DataType]) createChildPage(page interfaces.Page[DataType]) *BTChildPage[DataType] {
	return &BTChildPage[DataType]{Offset: page.Offset(), Page: page}
}
