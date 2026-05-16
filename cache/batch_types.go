package cache

type DeleteRequest struct {
	Key             Key
	ExpectedVersion uint64
}
