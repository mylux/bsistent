package interfaces

type PageChildren[DataType any] interface {
	All() []Page[DataType]
	ChildFor(Item[DataType]) Page[DataType]
	IsFetched() bool
	Last() Page[DataType]
	Nth(int) Page[DataType]
	Offsets() []int64
	Put(offset int64)
	Set(Page[DataType], ...Page[DataType]) PageChildren[DataType]
	Size() int
}
