package btree

import "github.com/mylux/bsistent/interfaces"

type BTPageItems[DataType any] struct {
	page interfaces.Page[DataType]
}

func (i *BTPageItems[DataType]) First() interfaces.Item[DataType] {
	return i.page.Item(0)
}

func (i *BTPageItems[DataType]) Last() interfaces.Item[DataType] {
	return i.page.Item(i.page.Size() - 1)
}

func (i *BTPageItems[DataType]) Item(index int) interfaces.Item[DataType] {
	return i.page.Item(index)
}

func (i *BTPageItems[DataType]) Split() ([]interfaces.Item[DataType], []interfaces.Page[DataType], []interfaces.Item[DataType], []interfaces.Page[DataType], interfaces.Item[DataType]) {
	items := i.page.Items().ToSlice()
	middle := (i.page.Size()) / 2
	left := make([]interfaces.Item[DataType], middle, middle+1)
	right := make([]interfaces.Item[DataType], middle, middle+1)
	childrenLeft := make([]interfaces.Page[DataType], 0, middle+1)
	childrenRight := make([]interfaces.Page[DataType], 0, middle+1)
	copy(left, items[:middle])
	copy(right, items[middle+1:])
	if !i.page.IsLeaf() {
		for x := 0; x <= middle; x++ {
			childrenLeft = append(childrenLeft, i.page.Child(x))
		}
		for x := middle + 1; x <= i.page.Capacity()+1; x++ {
			childrenRight = append(childrenRight, i.page.Child(x))
		}
	}

	return left, childrenLeft, right, childrenRight, items[middle]
}

func (i *BTPageItems[DataType]) ToSlice() []interfaces.Item[DataType] {
	pageSize := i.page.Size()
	r := make([]interfaces.Item[DataType], pageSize, i.page.Capacity()+1)
	for x := 0; x < pageSize; x++ {
		r[x] = i.page.Item(x)
	}
	return r
}

func (i *BTPageItems[DataType]) SlotFor(item interfaces.Item[DataType]) int {
	var it int = 0
	for it = 0; it < i.page.Size(); it++ {
		res, _ := i.Item(it).Compare(item)
		if res == 1 {
			return it
		}
	}
	return it
}
