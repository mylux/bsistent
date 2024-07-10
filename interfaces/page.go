package interfaces

type Page[DataType any] interface {
	Add(item Item[DataType])
	AddChild(child Page[DataType])
	AppendChildOffset(offset int64)
	Capacity() int
	ChildFor(item Item[DataType]) Page[DataType]
	Child(index int) Page[DataType]
	Children(children ...[]Page[DataType]) []Page[DataType]
	IsLeaf() bool
	Item(index int) Item[DataType]
	Items(items ...Item[DataType]) PageItems[DataType]
	GetChildrenStatus() (bool, []int64)
	IsFull() bool
	Offset() int64
	Parent(parent ...Page[DataType]) Page[DataType]
	Size() int
	String() string
	UnloadChildren()
}
