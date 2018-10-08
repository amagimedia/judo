package publisher

type JudoPub interface {
	Connect(map[string]interface{}) error
	Publish(string, []byte) error
	Close() error
}
