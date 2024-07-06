package interfaces

type Persistence[DataType any] interface {
	LoadRoot() (Page[DataType], error)
	Load(int64, ...bool) Page[DataType]
	NewPage(...bool) (Page[DataType], error)
	Save(Page[DataType]) error
	SaveRootReference(int64) error
	Reset()
}
