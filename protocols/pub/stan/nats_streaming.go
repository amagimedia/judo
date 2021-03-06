package stan

import (
	"fmt"
	"time"

	judoConfig "github.com/amagimedia/judo/v3/config"
	"github.com/amagimedia/judo/v3/publisher"
	gstan "github.com/nats-io/go-nats-streaming"
)

type Config struct {
	Name     string
	Topic    string
	Endpoint string
	Cluster  string
	AckTime  int
}

var natsSmap = map[string]string{
	"name":     "Name",
	"topic":    "Topic",
	"endpoint": "Endpoint",
	"cluster":  "Cluster",
	"ack_time": "AckTime",
}

func (c *Config) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"cluster",
		"ack_time",
	}
}

func (c *Config) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"cluster",
		"ack_time",
	}
}

func (c *Config) GetField(key string) string {
	return natsSmap[key]
}

type stanPub struct {
	Client    gstan.Conn
	connected bool
}

func (pub *stanPub) Connect(configs []interface{}) error {

	config := &Config{}
	cfgHelper := judoConfig.ConfigHelper{config}

	err := cfgHelper.ValidateAndSet(configs[0].(map[string]interface{}))
	if err != nil {
		return err
	}

	pub.Client, err = gstan.Connect(
		config.Cluster,
		config.Name,
		gstan.NatsURL(config.Endpoint),
		gstan.PubAckWait(time.Millisecond*time.Duration(config.AckTime)),
		gstan.SetConnectionLostHandler(pub.disconnected),
	)
	if err != nil {
		return err
	}
	pub.connected = true

	return nil
}

func (pub *stanPub) Publish(subject string, msg []byte) error {
	if pub.isConnected() {
		err := pub.Client.Publish(subject, msg)
		return err
	}
	return fmt.Errorf("Unable to publish message, disconnected from server.")
}

func (pub *stanPub) Close() error {
	return pub.Client.Close()
}

func (pub *stanPub) isConnected() bool {
	return pub.connected
}

func (pub *stanPub) disconnected(conn gstan.Conn, err error) {
	pub.connected = false
}

func New() (publisher.JudoPub, error) {
	return &stanPub{}, nil
}
