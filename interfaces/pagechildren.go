package interfaces

type PageChildren[DataType any] interface {
	All() []Page[DataType]
	BySize(...bool) PageChildren[DataType]
	ChildFor(Item[DataType]) Page[DataType]
	First() Page[DataType]
	Insert(Page[DataType], int)
	IsFetched() bool
	Last() Page[DataType]
	LookUp(Page[DataType]) int
	Nth(int) Page[DataType]
	Offsets() []int64
	Pick(...int) PageChildren[DataType]
	Popped(int) (int64, PageChildren[DataType])
	Put(int64, ...int)
	Set(Page[DataType], ...Page[DataType]) PageChildren[DataType]
	Siblings(Page[DataType]) PageChildren[DataType]
	Size() int
	Split(int) ([]Page[DataType], []Page[DataType])
	Unload()
}
