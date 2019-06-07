package reply

import (
	"github.com/amagimedia/judo/v2/client"
	judoConfig "github.com/amagimedia/judo/v2/config"
	jmsg "github.com/amagimedia/judo/v2/message"
	mangoRep "github.com/go-mangos/mangos/protocol/rep"
	"github.com/go-mangos/mangos/transport/ipc"
	"github.com/go-mangos/mangos/transport/tcp"
)

type nanoConnector func() (jmsg.RawSocket, error)

var nanomap = map[string]string{
	"name":      "Name",
	"topic":     "Topic",
	"endpoint":  "Endpoint",
	"separator": "Separator",
}

type NanoReply struct {
	connector  nanoConnector
	connection jmsg.RawSocket
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

func NewNanoReply() *NanoReply {
	rep := &NanoReply{connector: nanoConnect}
	return rep
}

func (rep *NanoReply) Configure(config map[string]interface{}) error {

	var err error

	configHelper := judoConfig.ConfigHelper{&rep.nanoConfig}
	err = configHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	return err
}

func (rep *NanoReply) Close() {
	rep.connection.Close()
}

func (rep *NanoReply) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	rep.callback = callback
	return rep
}

func (rep *NanoReply) Start() (<-chan error, error) {

	var err error
	errorChannel := make(chan error)
	rep.connection, err = rep.connector()

	if err != nil {
		return errorChannel, err
	}

	rep.connection.AddTransport(ipc.NewTransport())
	rep.connection.AddTransport(tcp.NewTransport())
	err = rep.connection.Listen(rep.nanoConfig.Endpoint)

	if err != nil {
		return errorChannel, err
	}

	go rep.receive(errorChannel)

	return errorChannel, err
}

func (rep *NanoReply) receive(ec chan error) {
	for {
		msg, err := rep.connection.Recv()
		if err != nil {
			ec <- err
			return
		}
		message := jmsg.NanoMessage{jmsg.NanoRawMessage{msg}, rep.connection, make(map[string]string)}
		rep.callback(message)
	}
}

func nanoConnect() (jmsg.RawSocket, error) {
	socket, err := mangoRep.NewSocket()
	if err != nil {
		return jmsg.NanoRawSocket{}, err
	}

	return jmsg.NanoRawSocket{socket}, nil
}
