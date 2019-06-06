package sub

import (
	"fmt"
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
	Persistence  bool
	FileName     string
}

func (c pubnubConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"subscribe_key",
		"persistence",
	}
}

func (c pubnubConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"subscribe_key",
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
	sub.connection.Destroy()
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

	go sub.handleMessage(errorChannel)

	go sub.receive(errorChannel)

	// If persistence is true then retrieve older messages on restart.
	if sub.pubnubConfig.Persistence && loadErr == nil {
		go sub.getMissingMessages()
	}

	return errorChannel, err
}

func (sub *PubnubSubscriber) receive(ec chan error) {
	var recvChannel chan *pubnub.PNMessage
	for listener, _ := range sub.connection.GetListeners() {
		recvChannel = listener.Message
	}
	for msg := range recvChannel {
		sub.processChannel <- sub.calcTimestamp(msg.Timetoken, msg.Message)
	}
	ec <- fmt.Errorf("Receive channel closed, Subscription ended.")
	sub.Close()
}

func (sub *PubnubSubscriber) handleMessage(ec chan error) {
	for message := range sub.processChannel {
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
	data, err := ioutil.ReadFile(".agent_msg_time." + sub.pubnubConfig.FileName)
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
	err := ioutil.WriteFile(".agent_msg_time."+sub.pubnubConfig.FileName, []byte(strconv.FormatInt(sub.lastMessageTime, 10)), 0755)
	if err != nil {
		return err
	}
	return nil
}

func pubnubConnect(cfg pubnubConfig) (jmsg.RawPubnubClient, error) {
	config := pubnub.NewConfig()
	config.SubscribeKey = cfg.SubscribeKey
	pubnubClient := pubnub.NewPubNub(config)
	listener := pubnub.NewListener()

	pubnubClient.AddListener(listener)

	return jmsg.PubnubRawClient{pubnubClient}, nil
}
