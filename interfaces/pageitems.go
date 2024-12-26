package interfaces

type PageItems[DataType any] interface {
	First() Item[DataType]
	Last() Item[DataType]
	Item(int) Item[DataType]
	Lookup(Item[DataType]) int
	Pop(int) Item[DataType]
	Split() ([]Item[DataType], []Item[DataType], int)
	ToSlice() []Item[DataType]
	SlotFor(Item[DataType]) int
}
