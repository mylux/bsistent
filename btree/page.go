package btree

import (
	"fmt"
	"slices"

	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
	"github.com/samber/lo"
)

type BTPage[DataType any] struct {
	offset   int64
	capacity int
	items    []interfaces.Item[DataType]
	parent   interfaces.Page[DataType]
	children interfaces.PageChildren[DataType]
}

func page[DataType any](offset int64, capacity int) interfaces.Page[DataType] {
	r := &BTPage[DataType]{
		capacity: capacity,
		items:    genPageEmptyItemList[DataType](capacity),
		offset:   offset,
		children: &BTPageChildren[DataType]{children: make([]*BTChildPage[DataType], 0, capacity+2)},
	}
	return r
}

func genPageEmptyItemList[DataType any](capacity int) []interfaces.Item[DataType] {
	return make([]interfaces.Item[DataType], 0, capacity+1)
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

func (b *BTPage[DataType]) Delta(p interfaces.Page[DataType]) interfaces.PageDelta {
	if b.IsEmpty() || p.IsEmpty() {
		return NewPageDelta(0)
	}
	return NewPageDelta(utils.OnError(func() (int, error) { return b.Item(0).Compare(p.Item(0)) }, 0))
}

func (b *BTPage[DataType]) EmptyItems() {
	b.items = genPageEmptyItemList[DataType](b.Capacity())
}

func (b *BTPage[DataType]) GiveChild(index int, whom interfaces.Page[DataType], left ...bool) {
	pos := 0
	if !utils.CoalesceBool(left) {
		pos = whom.Children().Size()
	}
	if !b.IsLeaf() {
		childToGive, updatedChildren := b.Children().Popped(index)
		b.children = updatedChildren
		whom.Children().Put(childToGive, pos)
	}
}

func (b *BTPage[DataType]) GiveChildren(whom interfaces.Page[DataType], prepend ...bool) {
	i := utils.Ternary(utils.CoalesceBool(prepend), b.Children().Size(), 0)
	for range b.Children().Size() {
		i = max(0, i-1)
		b.GiveChild(i, whom, prepend...)
	}
}

func (b *BTPage[DataType]) GiveItem(index int, whom interfaces.Page[DataType]) {
	whom.Add(b.Items().Pop(index))
}

func (b *BTPage[DataType]) GiveItems(whom interfaces.Page[DataType]) {
	for i := range b.Size() {
		b.GiveItem(i, whom)
	}
}

func (b *BTPage[DataType]) IsEmpty() bool {
	return b.Size() == 0
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

func (b *BTPage[DataType]) NotSame(page interfaces.Page[DataType]) bool {
	return !b.Same(page)
}

func (b *BTPage[DataType]) Offset() int64 {
	return b.offset
}

func (b *BTPage[DataType]) RemoveChild(child interfaces.Page[DataType]) {
	b.children.Popped(b.children.LookUp(child))
}

func (b *BTPage[DataType]) ResetParent() {
	b.parent = nil
}

func (b *BTPage[DataType]) Parent(parent ...interfaces.Page[DataType]) interfaces.Page[DataType] {
	if len(parent) > 0 {
		b.parent = parent[0]
	}
	return b.parent
}

func (b *BTPage[DataType]) ParentSlotFor(sibling interfaces.Page[DataType]) int {
	parent := b.Parent()
	if parent != nil && parent == sibling.Parent() {
		pageToLookup := lo.Ternary[interfaces.Page[DataType]](b.Delta(sibling).IsLeft(), b, sibling)
		return parent.Children().LookUp(pageToLookup)
	}
	return -1
}

func (b *BTPage[DataType]) Same(page interfaces.Page[DataType]) bool {
	return b.Offset() == page.Offset()
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
