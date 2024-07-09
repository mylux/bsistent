package interfaces

type Persistence[DataType any] interface {
	LoadRoot() (Page[DataType], error)
	Load(int64, ...bool) Page[DataType]
	LoadSize() (int64, error)
	NewPage(...bool) (Page[DataType], error)
	Reset()
	Save(Page[DataType]) error
	SaveRootReference(int64) error
	SaveSize(int64) error
}
