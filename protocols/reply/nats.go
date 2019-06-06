package reply

import (
	"errors"
	"fmt"
	"github.com/amagimedia/judo/v2/client"
	judoConfig "github.com/amagimedia/judo/v2/config"
	jmsg "github.com/amagimedia/judo/v2/message"
	nats "github.com/nats-io/go-nats"
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

type NatsReply struct {
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

func NewNatsReply() *NatsReply {
	rep := &NatsReply{connector: natsConnect, msgQueue: make(<-chan *nats.Msg)}
	return rep
}

func (rep *NatsReply) Configure(config map[string]interface{}) error {

	var err error

	configHelper := judoConfig.ConfigHelper{&rep.natsConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	url := rep.natsConfig.Endpoint
	if rep.natsConfig.User != "" && rep.natsConfig.Password != "" {
		url = fmt.Sprintf("%s:%s@%s", rep.natsConfig.User, rep.natsConfig.Password, rep.natsConfig.Endpoint)
	} else if rep.natsConfig.Token != "" {
		url = fmt.Sprintf("%s@%s", rep.natsConfig.Token, rep.natsConfig.Endpoint)
	}

	rep.connection, err = rep.connector(url)

	return err
}

func (rep *NatsReply) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	rep.callback = callback
	return rep
}

func (rep *NatsReply) Start() (<-chan error, error) {

	errorChannel := make(chan error)

	_, err := rep.connection.ChanSubscribe(rep.natsConfig.Topic, rep.msgQueue)
	if err != nil {
		return errorChannel, err
	}

	go rep.receive(errorChannel)

	return errorChannel, err
}

func (rep *NatsReply) Close() {
	rep.connection.Close()
}

func (rep *NatsReply) receive(ec chan error) {
	for msg := range rep.msgQueue {
		message := jmsg.NatsMessage{jmsg.NatsRawMessage{msg}, rep.connection, make(map[string]string)}
		rep.callback(message)
	}
	ec <- errors.New("Disconnected from nats server for " + rep.natsConfig.Name)
}

func natsConnect(url string) (jmsg.RawConnection, error) {
	nc, err := nats.Connect("nats://" + url)
	return jmsg.NatsRawConnection{*nc}, err
}
