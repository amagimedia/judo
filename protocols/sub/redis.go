package sub

import (
	"fmt"
	"github.com/amagimedia/judo/client"
	judoConfig "github.com/amagimedia/judo/config"
	jmsg "github.com/amagimedia/judo/message"
	gredis "github.com/go-redis/redis"
)

type redisConnector func(redisConfig) (jmsg.RawClient, error)

var redismap = map[string]string{
	"name":      "Name",
	"topic":     "Topic",
	"endpoint":  "Endpoint",
	"password":  "Password",
	"separator": "Separator",
}

type RedisSubscriber struct {
	connector  redisConnector
	connection jmsg.RawClient //gredis.Client
	redisConfig
	callback func(jmsg.Message)
}

type redisConfig struct {
	Name      string
	Topic     string
	Endpoint  string
	Password  string
	Separator string
}

func (c redisConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"password",
		"separator",
	}
}

func (c redisConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
	}
}

func (c redisConfig) GetField(key string) string {
	return redismap[key]
}

func NewRedisSub() *RedisSubscriber {
	sub := &RedisSubscriber{connector: redisConnect}
	return sub
}

func (sub *RedisSubscriber) Configure(config map[string]interface{}) error {

	var err error

	configHelper := judoConfig.ConfigHelper{&sub.redisConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	return err
}

func (sub *RedisSubscriber) Close() {
	sub.connection.Close()
}

func (sub *RedisSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	sub.callback = callback
	return sub
}

func (sub *RedisSubscriber) Start() (<-chan error, error) {

	var err error
	errorChannel := make(chan error)

	sub.connection, err = sub.connector(sub.redisConfig)
	if err != nil {
		return errorChannel, err
	}

	go sub.receive(errorChannel)

	return errorChannel, err
}

func (sub *RedisSubscriber) receive(ec chan error) {
	recvChannel := sub.connection.Channel()
	for msg := range recvChannel {
		message := jmsg.RedisMessage{jmsg.RedisRawMessage{msg}, sub.connection, make(map[string]string)}
		sub.callback(message)
	}
	ec <- fmt.Errorf("Receive channel closed, Subscription ended.")
}

func redisConnect(cfg redisConfig) (jmsg.RawClient, error) {
	redisClient := gredis.NewClient(&gredis.Options{
		Addr:     cfg.Endpoint,
		Password: cfg.Password,
	})

	return jmsg.RedisRawClient{redisClient, redisClient.Subscribe(cfg.Topic)}, nil
}
