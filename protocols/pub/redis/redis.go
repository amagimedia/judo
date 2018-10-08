package redis

import (
	judoConfig "github.com/amagimedia/judo/config"
	"github.com/amagimedia/judo/publisher"
	gredis "github.com/go-redis/redis"
)

type Config struct {
	Host     string
	Port     string
	Password string
	DB       int
}

func (c *Config) GetKeys() []string {
	return []string{"host", "port", "password", "db"}
}

func (c *Config) GetMandatoryKeys() []string {
	return []string{"host", "port"}
}

func (c *Config) GetField(key string) string {
	switch key {
	case "host":
		return "Host"
	case "port":
		return "Port"
	case "password":
		return "Password"
	case "db":
		return "DB"
	default:
		return ""
	}
	return ""
}

type redisPub struct {
	Client *gredis.Client
}

func (pub *redisPub) Connect(configs map[string]interface{}) error {

	config := &Config{}
	cfgHelper := judoConfig.ConfigHelper{config}

	err := cfgHelper.ValidateAndSet(configs)
	if err != nil {
		return err
	}

	pub.Client = gredis.NewClient(&gredis.Options{
		Addr: config.Host + ":" + config.Port,
	})
	return nil
}

func (pub *redisPub) Publish(subject string, msg []byte) error {
	errCap := pub.Client.Publish(subject, msg)
	return errCap.Err()
}

func (pub *redisPub) Close() error {
	return pub.Client.Close()
}

func New() (publisher.JudoPub, error) {
	return &redisPub{}, nil
}
