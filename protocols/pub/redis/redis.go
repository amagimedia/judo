package redis

import (
	judoConfig "github.com/amagimedia/judo/config"
	"github.com/amagimedia/judo/publisher"
	"github.com/amagimedia/judo/scripts"
	gredis "github.com/go-redis/redis"
	"time"
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

	err = pub.loadRedisScripts()

	return err
}

func (pub *redisPub) Publish(subject string, msg []byte) error {
	errCap := pub.Client.EvalSha(scripts.XPUBLISHSHA, make([]string, 0), subject, string(msg), time.Now().Unix())
	return errCap.Err()
}

func (pub *redisPub) Close() error {
	return pub.Client.Close()
}

func (pub *redisPub) loadRedisScripts() error {
	var res *gredis.StringCmd
	for _, script := range scripts.SHAtoCode {
		res = pub.Client.ScriptLoad(script)
		if res.Err() != nil {
			return res.Err()
		}
	}
	return nil
}

func New() (publisher.JudoPub, error) {
	return &redisPub{}, nil
}
