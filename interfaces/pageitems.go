package interfaces

type PageItems[DataType any] interface {
	First() Item[DataType]
	Last() Item[DataType]
	Item(index int) Item[DataType]
	Split() ([]Item[DataType], []Item[DataType], int)
	ToSlice() []Item[DataType]
	SlotFor(item Item[DataType]) int
}
