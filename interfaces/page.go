package interfaces

type Page[DataType any] interface {
	Add(item Item[DataType])
	AddChild(child Page[DataType])
	Capacity() int
	Child(index int) Page[DataType]
	Children(children ...[]Page[DataType]) PageChildren[DataType]
	IsLeaf() bool
	Item(index int) Item[DataType]
	Items(items ...Item[DataType]) PageItems[DataType]
	IsFull() bool
	Offset() int64
	Parent(parent ...Page[DataType]) Page[DataType]
	Size() int
	String() string
}
