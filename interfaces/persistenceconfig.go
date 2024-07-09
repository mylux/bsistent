package interfaces

type PersistenceConfig[DataType any] struct {
	Path            string
	PageConstructor func(int64) Page[DataType]
	ItemConstructor func() Item[DataType]
	CacheSize       uint32
}
