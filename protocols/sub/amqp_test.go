package sub

import (
	"errors"
	"github.com/amagimedia/judo/config"
	"github.com/amagimedia/judo/message"
	"github.com/amagimedia/judo/message/mocks"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

func TestAmqpSubscriber(t *testing.T) {
	fakeChannel := &mocks.RawChannel{}
	connector := func(c config.Config) (message.RawChannel, error) {
		return fakeChannel, nil
	}
	errConnector := func(c config.Config) (message.RawChannel, error) {
		return fakeChannel, errors.New("Cannot Create connection, Server not found")
	}
	fakeSubscriber := &AmqpSubscriber{connector: connector}

	cases := []struct {
		config  map[string]interface{}
		retVal  string
		retType error
	}{
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"success",
			nil,
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"error-ed",
			errors.New("Error in Exchange-declare"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"error-qd",
			errors.New("Error in Queue-declare"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"error-qb",
			errors.New("Error in Queue-bind"),
		},
		{
			map[string]interface{}{
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"error-cfg",
			errors.New("Key Missing : user"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"error-cfg-ex",
			errors.New("Key Missing : exchangeName"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"tag":          "test",
				"autoAck":      true,
			},
			"error-cfg-q",
			errors.New("Key Missing : routingKeys"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"autoAck":      true,
			},
			"error-cfg-q",
			errors.New("Key Missing : tag"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"tag":          "test",
				"routingKeys":  "blip.da",
				"autoAck":      true,
			},
			"error-conn",
			errors.New("Cannot Create connection, Server not found"),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"start-success",
			nil,
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"on-message",
			nil,
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"on-message-err",
			errors.New("Disconnected from server, subscriber closed."),
		},
		{
			map[string]interface{}{
				"user":         "guest",
				"password":     "MadFds@123",
				"host":         "localhost",
				"port":         "5672",
				"exchangeName": "blip_localhost",
				"exchangeType": "topic",
				"queueDurable": true,
				"queueNoWait":  true,
				"args":         nil,
				"queueName":    "amqp2",
				"routingKeys":  "blip.da",
				"tag":          "test",
				"autoAck":      true,
			},
			"consume-err",
			errors.New("Unable to consume from queue"),
		},
	}
	for _, c := range cases {
		switch c.retVal {
		case "success":
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("QueueBind", "", "blip.da", "blip_localhost", true, amqp.Table(nil)).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unable to Configure amqp", err.Error())
			}
			_ = NewAmqpSub()
		case "error-ed":
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(c.retType).Once()
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
		case "error-qd":
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, c.retType).Once()
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
		case "error-qb":
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("QueueBind", "", "blip.da", "blip_localhost", true, amqp.Table(nil)).Return(c.retType).Once()
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
		case "error-cfg":
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
			if err.Error() != c.retType.Error() {
				t.Error("Incorrect error thrown in ConfigHelper", err.Error())
			}
		case "error-cfg-ex":
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
			if err.Error() != c.retType.Error() {
				t.Error("Incorrect error thrown in ConfigHelper", err.Error())
			}
		case "error-cfg-q":
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
			if err.Error() != c.retType.Error() {
				t.Error("Incorrect error thrown in ConfigHelper", err.Error())
			}
		case "error-conn":
			fakeSubscriber.connector = errConnector
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
			if err.Error() != c.retType.Error() {
				t.Error("Incorrect error thrown in Connector func", err.Error())
			}
		case "start-success":
			fakeSubscriber.connector = connector
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("QueueBind", "", "blip.da", "blip_localhost", true, amqp.Table(nil)).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error in Configure", err.Error())
			}
			fakeChannel.On("Consume", "", "test", true, false, false, true, amqp.Table(nil)).Return(make(<-chan amqp.Delivery), nil).Once()
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("Error in Starting Consumer", err.Error())
			}
		case "on-message":
			rc := make(chan amqp.Delivery)

			cnv := func() <-chan amqp.Delivery {
				return rc
			}
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("QueueBind", "", "blip.da", "blip_localhost", true, amqp.Table(nil)).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error in Configure", err.Error())
			}
			called := false
			fakeSubscriber.OnMessage(func(msg message.Message) {
				called = true
			})

			fakeChannel.On("Consume", "", "test", true, false, false, true, amqp.Table(nil)).Return(cnv(), nil).Once()
			_, err = fakeSubscriber.Start()

			rc <- amqp.Delivery{}

			if !called {
				t.Error("Did not call on message")
			}

			fakeChannel.On("Close").Return(nil).Once()
			fakeSubscriber.Close()
		case "on-message-err":
			rc := make(chan amqp.Delivery)

			cnv := func() <-chan amqp.Delivery {
				return rc
			}
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("QueueBind", "", "blip.da", "blip_localhost", true, amqp.Table(nil)).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error in Configure", err.Error())
			}
			called := false
			fakeSubscriber.OnMessage(func(msg message.Message) {
				called = true
			})

			fakeChannel.On("Consume", "", "test", true, false, false, true, amqp.Table(nil)).Return(cnv(), nil).Once()
			ec, err := fakeSubscriber.Start()
			go func() {
				time.Sleep(time.Millisecond * 200)
				close(rc)
			}()
			err = <-ec
			if err.Error() != c.retType.Error() {
				t.Error("Invalid Error or nil error")
			}
		case "consume-err":
			fakeSubscriber.connector = connector
			fakeChannel.On("ExchangeDeclare", "blip_localhost", "topic", false, false, false, false, amqp.Table(nil)).Return(nil).Once()
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("QueueBind", "", "blip.da", "blip_localhost", true, amqp.Table(nil)).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error in Configure", err.Error())
			}
			fakeChannel.On("Consume", "", "test", true, false, false, true, amqp.Table(nil)).Return(make(<-chan amqp.Delivery), c.retType).Once()
			_, err = fakeSubscriber.Start()
			if err.Error() != c.retType.Error() {
				t.Error("Did not throw error when required.")
			}
		}
	}

}
