package utils

func PanicOnError(f func() error) {
	invokePanicOnError(f())
}

func ReturnOrPanic[T any](f func() (T, error)) T {
	r, err := f()
	invokePanicOnError(err)
	return r
}

func invokePanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
