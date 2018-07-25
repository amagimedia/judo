package sub

import (
	"errors"
	"fmt"
	nats "github.com/nats-io/go-nats"
	"gitlab.com/ajithnn/judo/client"
	judoConfig "gitlab.com/ajithnn/judo/config"
	jmsg "gitlab.com/ajithnn/judo/message"
)

var natsmap = map[string]string{
	"name":     "Name",
	"topic":    "Topic",
	"endpoint": "Endpoint",
	"user":     "User",
	"password": "Password",
	"token":    "Token",
}

type natsConnector func(string) (jmsg.RawConnection, error)

type NatsSubscriber struct {
	connector  natsConnector
	connection jmsg.RawConnection
	msgQueue   <-chan *nats.Msg
	natsConfig
	callback func(jmsg.Message)
}

type natsConfig struct {
	Name     string
	Topic    string
	Endpoint string
	User     string
	Password string
	Token    string
}

func (c natsConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"user",
		"password",
		"token",
	}
}

func (c natsConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
	}
}

func (c natsConfig) GetField(key string) string {
	return natsmap[key]
}

func NewNatsSub() *NatsSubscriber {
	sub := &NatsSubscriber{connector: natsConnect, msgQueue: make(<-chan *nats.Msg)}
	return sub
}

func (sub *NatsSubscriber) Configure(config map[string]interface{}) error {

	var err error

	configHelper := judoConfig.ConfigHelper{&sub.natsConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	url := sub.natsConfig.Endpoint
	if sub.natsConfig.User != "" && sub.natsConfig.Password != "" {
		url = fmt.Sprintf("%s:%s@%s", sub.natsConfig.User, sub.natsConfig.Password, sub.natsConfig.Endpoint)
	} else if sub.natsConfig.Token != "" {
		url = fmt.Sprintf("%s@%s", sub.natsConfig.Token, sub.natsConfig.Endpoint)
	}

	sub.connection, err = sub.connector(url)

	return err
}

func (sub *NatsSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	sub.callback = callback
	return sub
}

func (sub *NatsSubscriber) Start() (<-chan error, error) {

	errorChannel := make(chan error)

	_, err := sub.connection.ChanSubscribe(sub.natsConfig.Topic, sub.msgQueue)

	if err != nil {
		return errorChannel, err
	}

	go sub.receive(errorChannel)

	return errorChannel, err
}

func (sub *NatsSubscriber) Close() {
	sub.connection.Close()
}

func (sub *NatsSubscriber) receive(ec chan error) {
	for msg := range sub.msgQueue {
		message := jmsg.NatsMessage{jmsg.NatsRawMessage{msg}, sub.connection, make(map[string]string)}
		sub.callback(message)
	}
	ec <- errors.New("Disconnected, from server for " + sub.natsConfig.Name)
}

func natsConnect(url string) (jmsg.RawConnection, error) {
	connection, err := nats.Connect("nats://" + url)
	return jmsg.NatsRawConnection{*connection}, err
}
