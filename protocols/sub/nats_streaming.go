package sub

import (
	"fmt"
	"strings"

	"github.com/amagimedia/judo/v3/client"
	judoConfig "github.com/amagimedia/judo/v3/config"
	jmsg "github.com/amagimedia/judo/v3/message"
	"github.com/amagimedia/judo/v3/service"
	natsStream "github.com/nats-io/go-nats-streaming"
)

var natsSmap = map[string]string{
	"name":     "Name",
	"topic":    "Topic",
	"endpoint": "Endpoint",
	"cluster":  "Cluster",
	"user":     "User",
	"password": "Password",
	"token":    "Token",
}

type natsStreamConnector func(string, judoConfig.Config, func(natsStream.Conn, error)) (jmsg.RawConnection, error)

type NatsStreamSubscriber struct {
	connection jmsg.RawConnection
	connector  natsStreamConnector
	natsStreamConfig
	errorChannel chan error
	callback     func(jmsg.Message)
	deDuplifier  service.Duplicate
}

type natsStreamConfig struct {
	Name     string
	Topic    string
	Endpoint string
	Cluster  string
	User     string
	Password string
	Token    string
}

func (c natsStreamConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"cluster",
		"user",
		"password",
		"token",
	}
}

func (c natsStreamConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"cluster",
	}
}

func (c natsStreamConfig) GetField(key string) string {
	return natsSmap[key]
}

func NewNatsStreamSub() *NatsStreamSubscriber {
	sub := &NatsStreamSubscriber{connector: natsStreamConnect}
	return sub
}

func (sub *NatsStreamSubscriber) Configure(configs []interface{}) error {

	var err error
	config := configs[0].(map[string]interface{})
	configHelper := judoConfig.ConfigHelper{&sub.natsStreamConfig}
	err = configHelper.ValidateAndSet(config)

	if err != nil {
		return err
	}

	url := sub.natsStreamConfig.Endpoint
	if sub.natsStreamConfig.User != "" && sub.natsStreamConfig.Password != "" {
		url = fmt.Sprintf("%s:%s@%s", sub.natsStreamConfig.User, sub.natsStreamConfig.Password, sub.natsStreamConfig.Endpoint)
	} else if sub.natsStreamConfig.Token != "" {
		url = fmt.Sprintf("%s@%s", sub.natsStreamConfig.Token, sub.natsStreamConfig.Endpoint)
	}

	sub.connection, err = sub.connector(url, sub.natsStreamConfig, sub.errHandler)
	return err
}

func (sub *NatsStreamSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	sub.callback = callback
	return sub
}

func (sub *NatsStreamSubscriber) Start() (<-chan error, error) {

	_, err := sub.connection.Subscribe(
		sub.natsStreamConfig.Topic,
		sub.receive,
		natsStream.DurableName(sub.natsStreamConfig.Name),
		natsStream.SetManualAckMode(),
	)

	return sub.errorChannel, err
}

func (sub *NatsStreamSubscriber) Close() {
	sub.connection.Close()
}

func (sub *NatsStreamSubscriber) receive(msg *natsStream.Msg) {

	message := jmsg.NatsStreamMessage{jmsg.NatsStreamRawMessage{msg}, sub.connection, make(map[string]string)}
	messages := strings.Split(string(message.GetMessage()), "|")
	if len(messages) == 7 {
		messageString := strings.Replace(string(message.GetMessage()), messages[0]+"|", "", 1)
		sub.deDuplifier.UniqueID = messages[0]
		message.SetMessage([]byte(messageString))
	}
	if !sub.deDuplifier.IsDuplicate() {
		sub.callback(message)
	}

}

func (sub *NatsStreamSubscriber) errHandler(c natsStream.Conn, reason error) {
	sub.errorChannel <- reason
}

func natsStreamConnect(url string, c judoConfig.Config, handler func(natsStream.Conn, error)) (jmsg.RawConnection, error) {
	cfg := c.(natsStreamConfig)
	connection, err := natsStream.Connect(cfg.Cluster,
		cfg.Name,
		natsStream.NatsURL("nats://"+url),
		natsStream.SetConnectionLostHandler(handler),
	)
	return jmsg.NatsStreamRawConnection{connection}, err
}
