package nano

import (
	"fmt"
	judoConfig "github.com/amagimedia/judo/config"
	jmsg "github.com/amagimedia/judo/message"
	"github.com/amagimedia/judo/publisher"
	greq "github.com/go-mangos/mangos/protocol/req"
	"github.com/go-mangos/mangos/transport/ipc"
	gomangos "nanomsg.org/go-mangos"
	"time"
)

type Config struct {
	Name      string
	Topic     string
	Endpoint  string
	Separator string
	Timeout   int
}

var nanomap = map[string]string{
	"name":      "Name",
	"topic":     "Topic",
	"endpoint":  "Endpoint",
	"timeout":   "Timeout",
	"separator": "Separator",
}

func (c *Config) GetKeys() []string {
	return []string{
		"name",
		"topic",
		"endpoint",
		"timeout",
		"separator",
	}
}

func (c *Config) GetMandatoryKeys() []string {
	return []string{"name", "topic", "endpoint", "timeout"}
}

func (c *Config) GetField(key string) string {
	return nanomap[key]
}

type nanoReq struct {
	Socket jmsg.RawSocket
}

func (req *nanoReq) Connect(configs map[string]interface{}) error {

	config := &Config{}
	cfgHelper := judoConfig.ConfigHelper{config}

	err := cfgHelper.ValidateAndSet(configs)
	if err != nil {
		return err
	}

	req.Socket, err = greq.NewSocket()
	if err != nil {
		return err
	}

	req.Socket.AddTransport(ipc.NewTransport())
	err = req.Socket.Dial(config.Endpoint)
	if err != nil {
		return err
	}

	err = req.Socket.SetOption(gomangos.OptionRecvDeadline, time.Duration(config.Timeout)*time.Millisecond)
	if err != nil {
		return err
	}

	return nil
}

func (req *nanoReq) Publish(subject string, msg []byte) error {
	err := req.Socket.Send(msg)
	if err != nil {
		return err
	}

	rmsg, err := req.Socket.Recv()
	if err != nil {
		return err
	}

	if string(rmsg) != "OK" {
		return fmt.Errorf("Invalid ack. Please send 'OK'")
	}

	return nil

}

func (req *nanoReq) Close() error {
	return req.Socket.Close()
}

func New() (publisher.JudoPub, error) {
	return &nanoReq{}, nil
}
