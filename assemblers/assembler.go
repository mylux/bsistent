package assemblers

import (
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/persistence"
)

func NewPersistence[T any](config *interfaces.PersistenceConfig[T]) interfaces.Persistence[T] {
	return persistence.New[T](config)
}
