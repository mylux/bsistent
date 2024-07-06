package assemblers

import (
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/persistence"
)

func NewPersistence[T any](
	path string, pageCreator func(int64) interfaces.Page[T], itemCreator func() interfaces.Item[T]) interfaces.Persistence[T] {
	return persistence.New[T](path, pageCreator, itemCreator)
}
