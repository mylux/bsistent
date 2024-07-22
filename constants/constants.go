package constants

const PrintPrefix = "|-- "
const PrintSpacing = "    "

type bsistentFlags struct {
	Tag     string
	Key     string
	MaxSize string
}

var BsistentFlags bsistentFlags = bsistentFlags{
	Tag:     "bsistent",
	Key:     "key",
	MaxSize: "maxSize",
}
