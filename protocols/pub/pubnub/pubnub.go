package pubnub

import (
	judoConfig "github.com/amagimedia/judo/v2/config"
	"github.com/amagimedia/judo/v2/publisher"
	pubnub "github.com/pubnub/go"
)

type Config struct {
	SubscribeKey string
	PublishKey   string
}

func (c *Config) GetKeys() []string {
	return []string{"subscribe_key", "publish_key"}
}

func (c *Config) GetMandatoryKeys() []string {
	return []string{"subscribe_key", "publish_key"}
}

func (c *Config) GetField(key string) string {
	switch key {
	case "subscribe_key":
		return "SubscribeKey"
	case "publish_key":
		return "PublishKey"
	default:
		return ""
	}
	return ""
}

type pubnubPub struct {
	Client *pubnub.PubNub
}

func (pub *pubnubPub) Connect(configs []interface{}) error {

	config := &Config{}
	cfgHelper := judoConfig.ConfigHelper{config}
	err := cfgHelper.ValidateAndSet(configs[0].(map[string]interface{}))
	if err != nil {
		return err
	}

	cfg := pubnub.NewConfig()
	cfg.SubscribeKey = config.SubscribeKey
	cfg.PublishKey = config.PublishKey
	pub.Client = pubnub.NewPubNub(cfg)

	return err
}

func (pub *pubnubPub) Publish(subject string, msg []byte) error {
	mesg := map[string]interface{}{
		"msg": string(msg),
	}
	_, _, err := pub.Client.Publish().
		Channel(subject).
		Message(mesg).
		Execute()
	return err
}

func (pub *pubnubPub) Close() error {
	return nil
}

func New() (publisher.JudoPub, error) {
	return &pubnubPub{}, nil
}
