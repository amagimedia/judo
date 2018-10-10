package sub

import (
	"fmt"
	"github.com/amagimedia/judo/client"
	judoConfig "github.com/amagimedia/judo/config"
	jmsg "github.com/amagimedia/judo/message"
	gredis "github.com/go-redis/redis"
	"io/ioutil"
	"strconv"
	"time"
)

type redisConnector func(redisConfig) (jmsg.RawClient, error)

const (
	XPUBLISHSHA   = "e5b6a11ca0b32f7e7ee56347675966f77240a7bc"
	XSUBSCRIBESHA = "7a888fa9085114a6f4a8f3074cda494b564eb494"
)

var scripts = map[string]string{
	XPUBLISHSHA:   "local topic = ARGV[1];local msg = ARGV[2];local ts = ARGV[3];redis.call('PUBLISH', topic, msg);redis.call('ZADD', topic, ts, msg);return msg;",
	XSUBSCRIBESHA: "local topic = ARGV[1];local from = ARGV[2];local to = ARGV[3];return redis.call('ZRANGEBYSCORE', topic, from, to);",
}

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
	callback        func(jmsg.Message)
	processChannel  chan jmsg.RedisMessage
	lastMessageTime int64
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

	sub.processChannel = make(chan jmsg.RedisMessage)
	sub.loadLastTime()

	sub.connection, err = sub.connector(sub.redisConfig)
	if err != nil {
		return errorChannel, err
	}

	go sub.handleMessage(errorChannel)

	go sub.receive(errorChannel)

	go sub.getMissingMessages()

	return errorChannel, err
}

func (sub *RedisSubscriber) receive(ec chan error) {
	recvChannel := sub.connection.Channel()
	for msg := range recvChannel {
		message := jmsg.RedisMessage{
			jmsg.RedisRawMessage{msg},
			sub.connection,
			make(map[string]string),
		}
		sub.processChannel <- message
	}
	ec <- fmt.Errorf("Receive channel closed, Subscription ended.")
	sub.Close()
}

func (sub *RedisSubscriber) handleMessage(ec chan error) {
	for message := range sub.processChannel {
		sub.lastMessageTime = time.Now().UTC().Unix()
		sub.callback(message)
		err := sub.setLastTime()
		if err != nil {
			break
		}
	}
	sub.Close()
}

func (sub *RedisSubscriber) getMissingMessages() {
	resp := sub.connection.EvalSha(XSUBSCRIBESHA, make([]string, 0), sub.redisConfig.Topic, sub.lastMessageTime, time.Now().Unix())
	if resp.Err() != nil {
		return
	}
	result, err := resp.Result()
	if err != nil {
		return
	}
	for _, msg := range result.([]string) {
		fmt.Println(jmsg.RedisMessage{jmsg.RedisRawMessage{&gredis.Message{"", "", msg}}, sub.connection, make(map[string]string)})
		sub.processChannel <- jmsg.RedisMessage{jmsg.RedisRawMessage{&gredis.Message{"", "", msg}}, sub.connection, make(map[string]string)}
	}
}

func (sub *RedisSubscriber) loadLastTime() {
	data, err := ioutil.ReadFile(".agent_msg_time")
	if err != nil {
		sub.lastMessageTime = time.Now().UTC().Unix()
		return
	}
	t, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		sub.lastMessageTime = time.Now().UTC().Unix()
		return
	}
	sub.lastMessageTime = t
	return
}

func (sub *RedisSubscriber) setLastTime() error {
	err := ioutil.WriteFile(".agent_msg_time", []byte(strconv.FormatInt(sub.lastMessageTime, 10)), 0755)
	if err != nil {
		return err
	}
	return nil
}

func redisConnect(cfg redisConfig) (jmsg.RawClient, error) {
	redisClient := gredis.NewClient(&gredis.Options{
		Addr:     cfg.Endpoint,
		Password: cfg.Password,
	})

	return jmsg.RedisRawClient{redisClient, redisClient.Subscribe(cfg.Topic)}, nil
}
