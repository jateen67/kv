package internal

type Store interface {
	Get(key string) (string, error)
	Set(key *string, value *string) error
	Close() bool
}
