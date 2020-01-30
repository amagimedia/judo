package sub

import (
	"fmt"
	"os"

	"github.com/amagimedia/judo/v2/client"
	judoConfig "github.com/amagimedia/judo/v2/config"
	jmsg "github.com/amagimedia/judo/v2/message"
	pubnub "github.com/pubnub/go"
	"io/ioutil"
	"strconv"
	"strings"
)

type pubnubConnector func(pubnubConfig) (jmsg.RawPubnubClient, error)

var pubnubmap = map[string]string{
	"name":          "Name",
	"topic":         "Topic",
	"subscribe_key": "SubscribeKey",
	"publish_key":   "PublishKey",
	"secret_key":    "SecretKey",
	"persistence":   "Persistence",
}

type PubnubSubscriber struct {
	connector  pubnubConnector
	connection jmsg.RawPubnubClient //pubnub.Client
	pubnubConfig
	callback        func(jmsg.Message)
	processChannel  chan *jmsg.PubnubMessage
	lastMessageTime int64
}

type pubnubConfig struct {
	Name         string
	Topic        string
	SubscribeKey string
	PublishKey   string
	SecretKey    string
	Persistence  bool
	FileName     string
}

func (c pubnubConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"secret_key",
		"subscribe_key",
		"publish_key",
		"persistence",
	}
}

func (c pubnubConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"subscribe_key",
		"publish_key",
		"persistence",
	}
}

func (c pubnubConfig) GetField(key string) string {
	return pubnubmap[key]
}

func NewPubnubSub() *PubnubSubscriber {
	sub := &PubnubSubscriber{connector: pubnubConnect}
	return sub
}

func (sub *PubnubSubscriber) Configure(config map[string]interface{}) error {

	var err error

	configHelper := judoConfig.ConfigHelper{&sub.pubnubConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}
	sub.pubnubConfig.FileName = strings.Replace(sub.pubnubConfig.Topic, "/", "", -1)

	return err
}

func (sub *PubnubSubscriber) Close() {
	sub.connection.Destroy(sub.pubnubConfig.Topic)
}

func (sub *PubnubSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	sub.callback = callback
	return sub
}

func (sub *PubnubSubscriber) Start() (<-chan error, error) {
	var err error
	errorChannel := make(chan error)

	sub.processChannel = make(chan *jmsg.PubnubMessage)

	loadErr := sub.loadLastTime()

	sub.connection, err = sub.connector(sub.pubnubConfig)
	if err != nil {
		return errorChannel, err
	}

	go sub.receive(errorChannel)

	sub.connection.Subscribe(sub.pubnubConfig.Topic)

	go sub.handleMessage(errorChannel)

	if sub.pubnubConfig.Persistence && loadErr == nil {
		go sub.getMissingMessages()
	}

	return errorChannel, err
}

func (sub *PubnubSubscriber) receive(ec chan error) {
	listener := sub.connection.GetListener()
	for {
		select {
		case status := <-listener.Status:
			switch status.Category {
			case pubnub.PNConnectedCategory:
				for message := range listener.Message {
					sub.processChannel <- sub.calcTimestamp(message.Timetoken, message.Message)
					err := sub.setLastTime()
					if err != nil {
						break
					}
				}
				ec <- fmt.Errorf("Receive channel closed, Subscription ended.")
				sub.Close()
				return
			}
		}
	}
	ec <- fmt.Errorf("Receive channel closed, Subscription ended.")
	sub.Close()
	return
}

func (sub *PubnubSubscriber) handleMessage(ec chan error) {
	for message := range sub.processChannel {
		message.SetProperty("channel", sub.pubnubConfig.Topic)
		sub.callback(message)
		if val, ok := message.GetProperty("ack"); ok && val == "OK" {
			err := sub.setLastTime()
			if err != nil {
				break
			}
		}
	}
	sub.Close()
}

func (sub *PubnubSubscriber) getMissingMessages() {
	for true {
		messages, err := sub.connection.FetchHistory(sub.pubnubConfig.Topic, true, sub.lastMessageTime, true, 100)
		if err != nil {
			return
		}
		for _, m := range messages {
			sub.processChannel <- sub.calcTimestamp(m.Timetoken, m.Message)
			err := sub.setLastTime()
			if err != nil {
				break
			}
		}

		if len(messages) != 100 {
			break
		}
	}
}

func (sub *PubnubSubscriber) calcTimestamp(timetoken int64, msg interface{}) *jmsg.PubnubMessage {
	sub.lastMessageTime = timetoken
	return &jmsg.PubnubMessage{jmsg.PubnubRawMessage{&pubnub.PNMessage{Message: msg, Timetoken: timetoken}}, sub.connection, make(map[string]string)}
}

func (sub *PubnubSubscriber) loadLastTime() error {
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

func (sub *PubnubSubscriber) setLastTime() error {
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

func (sub *PubnubSubscriber) getPersistenceFilePath() string {
	filename := ".agent_msg_time." + sub.pubnubConfig.FileName
	folder := "/tmp/pubnub/"
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		os.Mkdir(folder, os.ModeDir)
	}
	return folder + filename
}

func pubnubConnect(cfg pubnubConfig) (jmsg.RawPubnubClient, error) {
	config := pubnub.NewConfig()
	config.SubscribeKey = cfg.SubscribeKey
	config.PublishKey = cfg.PublishKey
	if cfg.SecretKey != "" {
		config.SecretKey = cfg.SecretKey
	}

	return jmsg.PubnubRawClient{Client: pubnub.NewPubNub(config), Listener: pubnub.NewListener()}, nil
}
