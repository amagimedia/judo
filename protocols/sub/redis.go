package sub

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/amagimedia/judo/v3/client"
	judoConfig "github.com/amagimedia/judo/v3/config"
	jmsg "github.com/amagimedia/judo/v3/message"
	"github.com/amagimedia/judo/v3/scripts"
	"github.com/amagimedia/judo/v3/service"
	gredis "github.com/go-redis/redis"
)

type redisConnector func(redisConfig) (jmsg.RawClient, error)

var redismap = map[string]string{
	"name":        "Name",
	"topic":       "Topic",
	"endpoint":    "Endpoint",
	"password":    "Password",
	"tls":         "Tls",
	"separator":   "Separator",
	"persistence": "Persistence",
}

type RedisSubscriber struct {
	connector  redisConnector
	connection jmsg.RawClient //gredis.Client
	redisConfig
	callback        func(jmsg.Message)
	processChannel  chan *jmsg.RedisMessage
	lastMessageTime int64
	deDuplifier     service.Duplicate
}

type redisConfig struct {
	Name        string
	Topic       string
	Endpoint    string
	Password    string
	Tls         bool
	Separator   string
	Persistence bool
	FileName    string
}

func (c redisConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"password",
		"tls",
		"separator",
		"persistence",
	}
}

func (c redisConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"persistence",
	}
}

func (c redisConfig) GetField(key string) string {
	return redismap[key]
}

func NewRedisSub() *RedisSubscriber {
	sub := &RedisSubscriber{connector: redisConnect}
	return sub
}

func (sub *RedisSubscriber) Configure(configs []interface{}) error {

	var err error
	config := configs[0].(map[string]interface{})
	configHelper := judoConfig.ConfigHelper{&sub.redisConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}
	sub.redisConfig.FileName = strings.Replace(sub.redisConfig.Topic, "/", "", -1)

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

	sub.processChannel = make(chan *jmsg.RedisMessage)

	loadErr := sub.loadLastTime()

	sub.connection, err = sub.connector(sub.redisConfig)
	if err != nil {
		return errorChannel, err
	}

	for _, script := range scripts.SHAtoCode {
		res := sub.connection.ScriptLoad(script)
		if res.Err() != nil {
			return errorChannel, res.Err()
		}
	}

	go sub.handleMessage(errorChannel)

	go sub.receive(errorChannel)

	// If persistence is true then retrieve older messages on restart.
	if sub.redisConfig.Persistence && loadErr == nil {
		go sub.getMissingMessages()
	}

	return errorChannel, err
}

func (sub *RedisSubscriber) receive(ec chan error) {
	recvChannel := sub.connection.Channel()
	for msg := range recvChannel {
		sub.processChannel <- sub.calcTimestamp(msg.Channel, msg.Pattern, msg.Payload)
	}
	ec <- fmt.Errorf("Receive channel closed, Subscription ended.")
	sub.Close()
}

func (sub *RedisSubscriber) handleMessage(ec chan error) {
	for message := range sub.processChannel {
		messages := strings.Split(string(message.GetMessage()), "|")
		if len(messages) == 7 {
			messageString := strings.Replace(string(message.GetMessage()), messages[0]+"|", "", 1)
			sub.deDuplifier.UniqueID = messages[0]
			message.SetMessage([]byte(messageString))
		}
		if !sub.deDuplifier.IsDuplicate() {
			sub.callback(message)
			if val, ok := message.GetProperty("ack"); ok && val == "OK" {
				err := sub.setLastTime()
				if err != nil {
					break
				}
			}
		}
	}
	sub.Close()
}

func (sub *RedisSubscriber) getMissingMessages() {
	resp := sub.connection.EvalSha(scripts.XSUBSCRIBESHA, []string{"{" + sub.redisConfig.Topic + "}.list"}, sub.redisConfig.Topic, sub.lastMessageTime)
	if resp.Err() != nil {
		return
	}
	result, err := resp.Result()
	if err != nil {
		return
	}
	for _, msg := range result.([]interface{}) {
		sub.processChannel <- sub.calcTimestamp("", "", msg.(string))
	}
}

func (sub *RedisSubscriber) calcTimestamp(channel, pattern, msg string) *jmsg.RedisMessage {
	msgStrings := strings.Split(msg, "|")
	sub.lastMessageTime, _ = strconv.ParseInt(msgStrings[0], 10, 64)
	return &jmsg.RedisMessage{jmsg.RedisRawMessage{&gredis.Message{channel, pattern, strings.Join(msgStrings[1:], "|")}}, sub.connection, make(map[string]string)}
}

func (sub *RedisSubscriber) loadLastTime() error {
	persistencePath := sub.getPersistenceFilePath()
	if persistencePath == "" {
		return fmt.Errorf("Unable to find path to write persistence data.")
	}

	data, err := ioutil.ReadFile(persistencePath)
	if err != nil {
		return err
	}
	t, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return err
	}
	sub.lastMessageTime = t
	return nil
}

func (sub *RedisSubscriber) setLastTime() error {
	persistencePath := sub.getPersistenceFilePath()
	if persistencePath == "" {
		return fmt.Errorf("Unable to find path to write persistence data.")
	}

	err := ioutil.WriteFile(persistencePath, []byte(strconv.FormatInt(sub.lastMessageTime, 10)), 0755)
	if err != nil {
		return err
	}
	return nil
}

func (sub *RedisSubscriber) getPersistenceFilePath() string {
	filename := ".agent_msg_time." + sub.redisConfig.FileName
	folder := "/tmp/pubnub/"
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.Mkdir(folder, 0770)
	}
	return folder + filename
}

func redisConnect(cfg redisConfig) (jmsg.RawClient, error) {
	var redisClient *gredis.Client
	if cfg.Tls {

		redisClient = gredis.NewClient(&gredis.Options{
			Addr:      cfg.Endpoint,
			Password:  cfg.Password,
			TLSConfig: &tls.Config{},
		})

	} else {

		redisClient = gredis.NewClient(&gredis.Options{
			Addr:     cfg.Endpoint,
			Password: cfg.Password,
		})

	}

	return jmsg.RedisRawClient{redisClient, redisClient.Subscribe(cfg.Topic)}, nil
}
