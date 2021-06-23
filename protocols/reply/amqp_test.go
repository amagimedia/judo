package reply

import (
	"errors"
	"testing"
	"time"

	"github.com/amagimedia/judo/v3/config"
	"github.com/amagimedia/judo/v3/message"
	"github.com/amagimedia/judo/v3/message/mocks"
	"github.com/streadway/amqp"
)

func TestAmqpSubscriber(t *testing.T) {
	var called bool
	fakeChannel := &mocks.RawChannel{}
	connector := func(c config.Config) (message.RawChannel, error) {
		return fakeChannel, nil
	}
	errConnector := func(c config.Config) (message.RawChannel, error) {
		return fakeChannel, errors.New("Cannot Create connection, Server not found")
	}
	fakeSubscriber := &AmqpReply{connector: connector}

	cases := []struct {
		config  []interface{}
		retVal  string
		retType error
	}{
		{
			[]interface{}{
				map[string]interface{}{
					"user":        "guest",
					"password":    "MadFds@123",
					"host":        "localhost",
					"port":        "5672",
					"queueNoWait": true,
					"args":        nil,
					"queueName":   "amqp2",
					"routingKeys": "blip.da",
					"tag":         "test",
					"autoAck":     true,
				},
			},
			"success",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"error-qd",
			errors.New("Error in Queue-declare"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"error-qos",
			errors.New("Error in Qos"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"error-cfg",
			errors.New("Key Missing : user"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"autoAck":      true,
				},
			},
			"error-cfg-q",
			errors.New("Key Missing : tag"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"tag":          "test",
					"routingKeys":  "blip.da",
					"autoAck":      true,
				},
			},
			"error-conn",
			errors.New("Cannot Create connection, Server not found"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"start-success",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"on-message",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"on-message-err",
			errors.New("Disconnected from server, connection closed."),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"user":         "guest",
					"password":     "MadFds@123",
					"host":         "localhost",
					"port":         "5672",
					"queueDurable": true,
					"queueNoWait":  true,
					"args":         nil,
					"queueName":    "amqp2",
					"routingKeys":  "blip.da",
					"tag":          "test",
					"autoAck":      true,
				},
			},
			"consume-err",
			errors.New("Unable to consume from queue"),
		},
	}
	for _, c := range cases {
		switch c.retVal {
		case "success":
			fakeChannel.On("QueueDeclare", "amqp2", false, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("Qos", 1, 0, false).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unable to Configure amqp", err.Error())
			}
			_ = NewAmqpReply()
		case "error-qd":
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, c.retType).Once()
			err := fakeSubscriber.Configure(c.config)
			if err == nil {
				t.Error("Did not throw error correctly", err.Error())
			}
		case "error-qos":
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("Qos", 1, 0, false).Return(c.retType).Once()
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
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("Qos", 1, 0, false).Return(nil).Once()
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
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("Qos", 1, 0, false).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error in Configure", err.Error())
			}
			called = false
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
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("Qos", 1, 0, false).Return(nil).Once()
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error in Configure", err.Error())
			}
			called = false
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
				t.Error("Invalid Error or nil error", err.Error())
			}
		case "consume-err":
			fakeSubscriber.connector = connector
			fakeChannel.On("QueueDeclare", "amqp2", true, false, false, true, amqp.Table(nil)).Return(amqp.Queue{}, nil).Once()
			fakeChannel.On("Qos", 1, 0, false).Return(nil).Once()
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
