package sub

import (
	mangoSub "github.com/go-mangos/mangos/protocol/sub"
	"github.com/go-mangos/mangos/transport/ipc"
	"github.com/go-mangos/mangos/transport/tcp"
	"gitlab.com/ajithnn/judo/client"
	judoConfig "gitlab.com/ajithnn/judo/config"
	jmsg "gitlab.com/ajithnn/judo/message"
	mangos "nanomsg.org/go-mangos"
)

type nanoConnector func() (jmsg.RawSocket, error)

var nanomap = map[string]string{
	"name":      "Name",
	"topic":     "Topic",
	"endpoint":  "Endpoint",
	"separator": "Separator",
}

type NanoSubscriber struct {
	connector  nanoConnector
	connection jmsg.RawSocket //mangos.Socket
	nanoConfig
	callback func(jmsg.Message)
}

type nanoConfig struct {
	Name      string
	Topic     string
	Endpoint  string
	Separator string
}

func (c nanoConfig) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"separator",
	}
}

func (c nanoConfig) GetMandatoryKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
	}
}

func (c nanoConfig) GetField(key string) string {
	return nanomap[key]
}

func NewNanoSub() *NanoSubscriber {
	sub := &NanoSubscriber{connector: nanoConnect}
	return sub
}

func (sub *NanoSubscriber) Configure(config map[string]interface{}) error {

	var err error

	configHelper := judoConfig.ConfigHelper{&sub.nanoConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	return err
}

func (sub *NanoSubscriber) Close() {
	sub.connection.Close()
}

func (sub *NanoSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	sub.callback = callback
	return sub
}

func (sub *NanoSubscriber) Start() (<-chan error, error) {

	var err error
	errorChannel := make(chan error)

	sub.connection, err = sub.connector()
	if err != nil {
		return errorChannel, err
	}

	sub.connection.AddTransport(ipc.NewTransport())
	sub.connection.AddTransport(tcp.NewTransport())
	err = sub.connection.Dial(sub.nanoConfig.Endpoint)

	if err != nil {
		return errorChannel, err
	}

	err = sub.connection.SetOption(mangos.OptionSubscribe, []byte(sub.nanoConfig.Topic))
	if err != nil {
		return errorChannel, err
	}

	go sub.receive(errorChannel)

	return errorChannel, err
}

func (sub *NanoSubscriber) receive(ec chan error) {
	for {
		msg, err := sub.connection.Recv()
		if err != nil {
			ec <- err
			return
		}
		message := jmsg.NanoMessage{jmsg.NanoRawMessage{msg}, sub.connection, make(map[string]string)}
		sub.callback(message)
	}
}

func nanoConnect() (jmsg.RawSocket, error) {
	socket, err := mangoSub.NewSocket()
	if err != nil {
		return jmsg.NanoRawSocket{}, err
	}

	return jmsg.NanoRawSocket{socket}, nil
}
