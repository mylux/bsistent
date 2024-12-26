package interfaces

type Page[DataType any] interface {
	Add(item Item[DataType])
	AddChild(child Page[DataType])
	Capacity() int
	Child(index int) Page[DataType]
	Children(children ...[]Page[DataType]) PageChildren[DataType]
	Delta(Page[DataType]) PageDelta
	EmptyItems()
	GiveChild(int, Page[DataType], ...bool)
	GiveChildren(Page[DataType], ...bool)
	GiveItem(int, Page[DataType])
	GiveItems(Page[DataType])
	NotSame(Page[DataType]) bool
	IsEmpty() bool
	IsFull() bool
	IsLeaf() bool
	Item(index int) Item[DataType]
	Items(items ...Item[DataType]) PageItems[DataType]
	Offset() int64
	Parent(parent ...Page[DataType]) Page[DataType]
	ParentSlotFor(Page[DataType]) int
	RemoveChild(Page[DataType])
	ResetParent()
	Same(page Page[DataType]) bool
	Size() int
	String() string
}
