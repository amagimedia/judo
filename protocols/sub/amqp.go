package sub

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/amagimedia/judo/v3/client"
	judoConfig "github.com/amagimedia/judo/v3/config"
	jmsg "github.com/amagimedia/judo/v3/message"
	"github.com/amagimedia/judo/v3/service"
	gredis "github.com/go-redis/redis"
	"github.com/streadway/amqp"
)

var amqpmap = map[string]string{
	"queueName":          "QueueName",
	"queueDurable":       "QueueDurable",
	"queueAutoDelete":    "QueueAutoDelete",
	"queueNoWait":        "QueueNoWait",
	"routingKeys":        "RoutingKeys",
	"tag":                "Tag",
	"autoAck":            "AutoAck",
	"exclusive":          "Exclusive",
	"noLocal":            "NoLocal",
	"exchangeName":       "ExchangeName",
	"exchangeDurable":    "ExchangeDurable",
	"exchangeAutoDelete": "ExchangeAutoDelete",
	"exchangeNoWait":     "ExchangeNoWait",
	"internal":           "Internal",
	"exchangeType":       "ExchangeType",
	"args":               "Args",
	"user":               "User",
	"password":           "Password",
	"host":               "Host",
	"port":               "Port",
}

type amqpConnector func(judoConfig.Config) (jmsg.RawChannel, error)

type AmqpSubscriber struct {
	connector amqpConnector
	channel   jmsg.RawChannel
	queue     amqp.Queue
	msgQueue  <-chan amqp.Delivery
	amqpConfig
	callback    func(jmsg.Message)
	deDuplifier service.Duplicate
}

type amqpConfig struct {
	User               string
	Password           string
	Host               string
	Port               string
	ExchangeName       string
	ExchangeType       string
	ExchangeDurable    bool
	ExchangeAutoDelete bool
	Internal           bool
	ExchangeNoWait     bool
	QueueName          string
	RoutingKeys        []string
	Tag                string
	QueueDurable       bool
	QueueAutoDelete    bool
	AutoAck            bool
	Exclusive          bool
	QueueNoWait        bool
	NoLocal            bool
	Args               amqp.Table
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
		"routingKeys",
		"tag",
		"autoAck",
		"exclusive",
		"noLocal",
		"exchangeName",
		"exchangeType",
		"exchangeDurable",
		"exchangeAutoDelete",
		"internal",
		"exchangeNoWait",
		"args",
	}
}

func (c amqpConfig) GetMandatoryKeys() []string {
	return []string{
		"user",
		"password",
		"host",
		"port",
		"queueName",
		"routingKeys",
		"tag",
		"exchangeName",
		"exchangeType",
	}
}

func (c amqpConfig) GetField(key string) string {
	return amqpmap[key]
}

func NewAmqpSub() *AmqpSubscriber {
	sub := &AmqpSubscriber{connector: amqpConnect}
	return sub
}

func (sub *AmqpSubscriber) Start() (<-chan error, error) {

	var err error
	errorChannel := make(chan error)

	sub.msgQueue, err = sub.channel.Consume(
		sub.queue.Name,             // queue
		sub.amqpConfig.Tag,         // consumer
		sub.amqpConfig.AutoAck,     // consumer
		sub.amqpConfig.Exclusive,   // consumer
		sub.amqpConfig.NoLocal,     // consumer
		sub.amqpConfig.QueueNoWait, // consumer
		sub.amqpConfig.Args,        // consumer
	)

	if err != nil {
		return errorChannel, err
	}

	go func() {
		for msg := range sub.msgQueue {
			wrappedMsg := jmsg.AmqpMessage{jmsg.AmqpRawMessage{msg}, sub.channel, make(map[string]string)}
			messages := strings.Split(string(wrappedMsg.GetMessage()), "|")
			if len(messages) == 6 {
				sub.deDuplifier.EventID = messages[len(messages)-1]
				sub.deDuplifier.TimeStamp, _ = strconv.ParseInt(messages[3], 10, 0)
			}
			if !sub.deDuplifier.IsDuplicate() {
				wrappedMsg.SetProperty("protocol_type", "subscribe")
				sub.callback(wrappedMsg)
			}
		}
		errorChannel <- errors.New("Disconnected from server, subscriber closed.")
	}()

	return errorChannel, nil
}

func (sub *AmqpSubscriber) Close() {
	sub.channel.Close()
}

func (sub *AmqpSubscriber) OnMessage(callback func(jmsg.Message)) client.JudoClient {
	sub.callback = callback
	return sub
}

func (sub *AmqpSubscriber) Configure(configs []interface{}) error {

	// extract connection details from config and call connect
	var err error
	config := configs[0].(map[string]interface{})
	if _, ok := config["routingKeys"]; !ok {
		return errors.New("Key Missing : routingKeys")
	}
	config["routingKeys"] = strings.Split(config["routingKeys"].(string), ",")

	cfgHelper := judoConfig.ConfigHelper{&sub.amqpConfig}
	err = cfgHelper.ValidateAndSet(config)
	if err != nil {
		return err
	}

	sub.channel, err = sub.connector(&sub.amqpConfig)
	if err != nil {
		return err
	}

	err = sub.setup(sub.amqpConfig)

	if _, ok := os.LookupEnv("REDIS_URL"); ok {
		sub.deDuplifier.RedisConn = gredis.NewClient(&gredis.Options{
			Addr:     os.Getenv("REDIS_URL"),
			Password: os.Getenv("REDIS_PASSWORD"),
		})
	}

	return err
}

func (sub *AmqpSubscriber) setup(c amqpConfig) error {

	var err error
	err = sub.channel.ExchangeDeclare(
		c.ExchangeName,
		c.ExchangeType,
		c.ExchangeDurable,
		c.ExchangeAutoDelete,
		c.Internal,
		c.ExchangeNoWait,
		c.Args,
	)

	if err != nil {
		return err
	}

	sub.queue, err = sub.channel.QueueDeclare(
		c.QueueName,
		c.QueueDurable,
		c.QueueAutoDelete,
		c.Exclusive,
		c.QueueNoWait,
		c.Args,
	)

	if err != nil {
		return err
	}

	for _, key := range c.RoutingKeys {
		err = sub.channel.QueueBind(
			sub.queue.Name, // queue name
			key,
			c.ExchangeName,
			c.QueueNoWait,
			c.Args,
		)
		if err != nil {
			return err
		}
	}

	return nil

}

func amqpConnect(cfg judoConfig.Config) (jmsg.RawChannel, error) {

	var err error
	connCfg := cfg.(*amqpConfig)
	connectTo := fmt.Sprintf("amqp://%s:%s@%s:%s/", connCfg.User, connCfg.Password, connCfg.Host, connCfg.Port)
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
