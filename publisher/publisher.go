package publisher

type JudoPub interface {
	Connect([]interface{}) error
	Publish(string, []byte) error
	Close() error
}
