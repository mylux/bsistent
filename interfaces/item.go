package interfaces

type Item[DataType any] interface {
	Capacity() int64
	Content() DataType
	Compare(Item[DataType]) (int, error)
	IsEmpty() bool
	Load(DataType) Item[DataType]
	String() string
}
