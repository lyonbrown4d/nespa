package runtime

type EmptyInput struct{}

type JSONResponse[T any] struct {
	Body T
}

func JSON[T any](body T) *JSONResponse[T] {
	return &JSONResponse[T]{Body: body}
}
