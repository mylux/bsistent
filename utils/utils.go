package utils

func PanicOnError(f func() error) {
	invokePanicOnError(f())
}

func ReturnOrPanic[T any](f func() (T, error)) T {
	r, err := f()
	invokePanicOnError(err)
	return r
}

func OnError[T any](f func() (T, error), d T) T {
	r, err := f()
	if err != nil {
		return d
	}
	return r
}

func invokePanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
