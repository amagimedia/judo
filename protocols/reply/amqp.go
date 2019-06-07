package reply

import (
	"errors"
	"fmt"
	"github.com/amagimedia/judo/v2/client"
	judoConfig "github.com/amagimedia/judo/v2/config"
	jmsg "github.com/amagimedia/judo/v2/message"
	"github.com/streadway/amqp"
)

type amqpConnector func(judoConfig.Config) (jmsg.RawChannel, error)

type AmqpReply struct {
	channel   jmsg.RawChannel
	connector amqpConnector
	queue     amqp.Queue
	msgQueue  <-chan amqp.Delivery
	amqpConfig
	callback func(jmsg.Message)
}

var amqpmap = map[string]string{
	"queueName":       "QueueName",
	"queueDurable":    "Durable",
	"queueAutoDelete": "AutoDelete",
	"queueNoWait":     "NoWait",
	"tag":             "Tag",
	"autoAck":         "AutoAck",
	"exclusive":       "Exclusive",
	"noLocal":         "NoLocal",
	"args":            "Args",
	"user":            "User",
	"password":        "Password",
	"host":            "Host",
	"port":            "Port",
}

type amqpConfig struct {
	User       string
	Password   string
	Host       string
	Port       string
	QueueName  string
	Tag        string
	Durable    bool
	AutoDelete bool
	AutoAck    bool
	Exclusive  bool
	NoWait     bool
	NoLocal    bool
	Args       amqp.Table
}

func (c amqpConfig) GetKeys() []string {
	return []string{
		"user",
		"password",
		"host",
		"port",
		"queueName",
		"queueDurable",
		"queueAutoDelete",
		"queueNoWait",
		"tag",
		"autoAck",
		"exclusive",
		"noLocal",
	}
}

func (c amqpConfig) GetMandatoryKeys() []string {
	return []string{
		"user",
		"password",
		"host",
		"port",
		"queueName",
		"tag",
	}
}

func (c amqpConfig) GetField(key string) string {
	return amqpmap[key]
}

func NewAmqpReply() *AmqpReply {
	rep := &AmqpReply{connector: amqpConnect}
	return rep
}

func (rep *AmqpReply) Start() (<-chan error, error) {

	var err error
	errorChannel := make(chan error)

	rep.msgQueue, err = rep.channel.Consume(
		rep.queue.Name,           // queue
		rep.amqpConfig.Tag,       // consumer
		rep.amqpConfig.AutoAck,   // consumer
		rep.amqpConfig.Exclusive, // consumer
		rep.amqpConfig.NoLocal,   // consumer
		rep.amqpConfig.NoWait,    // consumer
		rep.amqpConfig.Args,      // consumer
	)

	if err != nil {
		return errorChannel, err
	}

	go func() {
		for msg := range rep.msgQueue {
			wrappedMsg := jmsg.AmqpMessage{jmsg.AmqpRawMessage{msg}, rep.channel, make(map[string]string)}
			wrappedMsg.SetProperty("protocol_type", "reqrep")
			rep.callback(wrappedMsg)
		}
		errorChannel <- errors.New("Disconnected from server, connection closed.")
	}()

	return errorChannel, nil
}

func (rep *AmqpReply) Close() {
	rep.channel.Close()
}

func (rep *AmqpReply) OnMessage(callback func(jmsg.Message)) client.JudoClient {
	rep.callback = callback
	return rep
}

func (rep *AmqpReply) Configure(config map[string]interface{}) error {

	// extract connection details from config and call connect
	var err error

	cfgHelper := judoConfig.ConfigHelper{&rep.amqpConfig}
	err = cfgHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	rep.channel, err = rep.connector(rep.amqpConfig)
	if err != nil {
		return err
	}

	err = rep.setup(rep.amqpConfig)

	return err
}

func (rep *AmqpReply) setup(c amqpConfig) error {
	var err error

	rep.queue, err = rep.channel.QueueDeclare(
		c.QueueName,
		c.Durable,
		c.AutoDelete,
		c.Exclusive,
		c.NoWait,
		c.Args,
	)

	if err != nil {
		return err
	}

	err = rep.channel.Qos(1, 0, false)

	if err != nil {
		return err
	}

	return nil

}

func amqpConnect(c judoConfig.Config) (jmsg.RawChannel, error) {

	var err error
	cfg := c.(amqpConfig)
	connectTo := fmt.Sprintf("amqp://%s:%s@%s:%s/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	connection, err := amqp.Dial(connectTo)

	if err != nil {
		return jmsg.AmqpRawChannel{}, err
	}

	channel, err := connection.Channel()

	if err != nil {
		return jmsg.AmqpRawChannel{}, err
	}
	return jmsg.AmqpRawChannel{channel}, nil
}
