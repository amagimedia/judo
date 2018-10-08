package message_test

import (
	"github.com/amagimedia/judo/message"
	"github.com/amagimedia/judo/message/mocks"
	"github.com/go-mangos/mangos/protocol/sub"
	nats "github.com/nats-io/go-nats"
	natsStream "github.com/nats-io/go-nats-streaming"
	"github.com/streadway/amqp"
	"testing"
)

func TestAmqpMessage(t *testing.T) {
	fakeRawMessage := &mocks.RawMessage{}
	fakeRawChannel := &mocks.RawChannel{}
	fakeMessage := &message.AmqpMessage{
		fakeRawMessage,
		fakeRawChannel,
		map[string]string{"protocol_type": "sub"},
	}

	cases := []struct {
		name         string
		propertyName string
		propertyVal  string
		msg          []byte
		ack          []byte
	}{
		{
			"set_property",
			"protocol_type",
			"sub",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_property",
			"protocol_type",
			"reply",
			[]byte(""),
			[]byte(""),
		},
		{
			"get_property",
			"random_string",
			"never_returned",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_message",
			"protocol_type",
			"sub",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"ack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"nack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("ERR"),
		},
	}

	for _, c := range cases {
		switch c.name {
		case "set_property":
			fakeMessage.SetProperty(c.propertyName, c.propertyVal)
			if val, ok := fakeMessage.GetProperty(c.propertyName); !ok || val != c.propertyVal {
				t.Error("Set Property failed to set approprate value")
			}
		case "get_property":
			if val, ok := fakeMessage.GetProperty(c.propertyName); ok && val == c.propertyVal {
				t.Error("Got Unset Property.")
			}
		case "set_message":
			fakeRawMessage.On("SetBody", c.msg).Return(mocks.RawMessage{})
			fakeRawMessage.On("GetBody").Return(c.msg)
			_ = fakeMessage.SetMessage(c.msg)
			if string(fakeMessage.GetMessage()) != string(c.msg) {
				t.Error("Failed to Set Message body")
			}
		case "ack":
			fakeMessage.SetProperty(c.propertyName, c.propertyVal)
			fakeRawMessage.On("GetReplyTo").Return("TestReplyTo").Once()
			fakeRawMessage.On("GetCorrelationId").Return("corelid").Once()
			fakeRawMessage.On("Ack", false).Return(nil).Once()
			fakeRawChannel.On("Publish", "", "TestReplyTo", false, false, amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: "corelid",
				Body:          c.ack,
			}).Return(nil).Once()
			fakeMessage.SendAck(c.ack)
		case "nack":
			fakeRawMessage.On("Nack", false, true).Return(nil).Once()
			fakeMessage.SendNack(c.ack)
		default:
			t.Error("Unknown case")
		}
	}

}

func TestAmqpRawWrapper(t *testing.T) {

	wrapMessage := message.AmqpRawMessage{}
	wrapMessage.Ack(false)
	wrapMessage.Nack(false, true)
	wrapMessage.GetBody()
	wrapMessage.SetBody([]byte(""))
	wrapMessage.GetReplyTo()
	wrapMessage.GetCorrelationId()

}

func TestNanoMessage(t *testing.T) {
	fakeRawMessage := &mocks.RawMessage{}
	fakeRawSocket := &mocks.RawSocket{}
	fakeMessage := &message.NanoMessage{
		fakeRawMessage,
		fakeRawSocket,
		map[string]string{"protocol_type": "sub"},
	}

	cases := []struct {
		name         string
		propertyName string
		propertyVal  string
		msg          []byte
		ack          []byte
	}{
		{
			"set_property",
			"protocol_type",
			"sub",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_property",
			"protocol_type",
			"reply",
			[]byte(""),
			[]byte(""),
		},
		{
			"get_property",
			"random_string",
			"never_returned",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_message",
			"protocol_type",
			"sub",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"ack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"nack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("ERR"),
		},
	}

	for _, c := range cases {
		switch c.name {
		case "set_property":
			fakeMessage.SetProperty(c.propertyName, c.propertyVal)
			if val, ok := fakeMessage.GetProperty(c.propertyName); !ok || val != c.propertyVal {
				t.Error("Set Property failed to set approprate value")
			}
		case "get_property":
			if val, ok := fakeMessage.GetProperty(c.propertyName); ok && val == c.propertyVal {
				t.Error("Got Unset Property.")
			}
		case "set_message":
			fakeRawMessage.On("SetBody", c.msg).Return(mocks.RawMessage{})
			fakeRawMessage.On("GetBody").Return(c.msg)
			_ = fakeMessage.SetMessage(c.msg)
			if string(fakeMessage.GetMessage()) != string(c.msg) {
				t.Error("Failed to Set Message body")
			}
		case "ack":
			fakeRawSocket.On("Send", c.ack).Return(nil).Once()
			fakeMessage.SendAck(c.ack)
		case "nack":
			fakeRawSocket.On("Send", c.ack).Return(nil).Once()
			fakeMessage.SendNack(c.ack)
		default:
			t.Error("Unknown case")
		}
	}

}

func TestNanoRawWrapper(t *testing.T) {

	wrapMessage := message.NanoRawMessage{}
	wrapMessage.Ack(false)
	wrapMessage.Nack(false, true)
	wrapMessage.GetBody()
	wrapMessage.SetBody([]byte(""))
	wrapMessage.GetReplyTo()
	wrapMessage.GetCorrelationId()
	s, _ := sub.NewSocket()
	wrapSocket := message.NanoRawSocket{s}
	wrapSocket.Send([]byte(""))
}

func TestNatsMessage(t *testing.T) {
	fakeRawMessage := &mocks.RawMessage{}
	fakeRawConnect := &mocks.RawConnection{}
	fakeMessage := &message.NatsMessage{
		fakeRawMessage,
		fakeRawConnect,
		map[string]string{"protocol_type": "sub"},
	}

	cases := []struct {
		name         string
		propertyName string
		propertyVal  string
		msg          []byte
		ack          []byte
	}{
		{
			"set_property",
			"protocol_type",
			"sub",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_property",
			"protocol_type",
			"reply",
			[]byte(""),
			[]byte(""),
		},
		{
			"get_property",
			"random_string",
			"never_returned",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_message",
			"protocol_type",
			"sub",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"ack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"nack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("ERR"),
		},
	}

	for _, c := range cases {
		switch c.name {
		case "set_property":
			fakeMessage.SetProperty(c.propertyName, c.propertyVal)
			if val, ok := fakeMessage.GetProperty(c.propertyName); !ok || val != c.propertyVal {
				t.Error("Set Property failed to set approprate value")
			}
		case "get_property":
			if val, ok := fakeMessage.GetProperty(c.propertyName); ok && val == c.propertyVal {
				t.Error("Got Unset Property.")
			}
		case "set_message":
			fakeRawMessage.On("SetBody", c.msg).Return(mocks.RawMessage{})
			fakeRawMessage.On("GetBody").Return(c.msg)
			_ = fakeMessage.SetMessage(c.msg)
			if string(fakeMessage.GetMessage()) != string(c.msg) {
				t.Error("Failed to Set Message body")
			}
		case "ack":
			fakeRawConnect.On("Publish", c.propertyVal, c.ack).Return(nil).Once()
			fakeRawMessage.On("GetReplyTo").Return(c.propertyVal).Once()
			fakeMessage.SendAck(c.ack)
		case "nack":
			fakeRawConnect.On("Publish", c.propertyVal, c.ack).Return(nil).Once()
			fakeRawMessage.On("GetReplyTo").Return(c.propertyVal).Once()
			fakeMessage.SendNack(c.ack)
		default:
			t.Error("Unknown case")
		}
	}

}

func TestNatsRawWrapper(t *testing.T) {

	wrapMessage := message.NatsRawMessage{&nats.Msg{}}
	wrapMessage.Ack(false)
	wrapMessage.Nack(false, true)
	wrapMessage.GetBody()
	wrapMessage.SetBody([]byte(""))
	wrapMessage.GetReplyTo()
	wrapMessage.GetCorrelationId()

	wrapConnection := message.NatsRawConnection{nats.Conn{}}
	wrapConnection.Publish("", []byte(""))

}

func TestNatsStreamingMessage(t *testing.T) {
	fakeRawMessage := &mocks.RawMessage{}
	fakeRawConnect := &mocks.RawConnection{}
	fakeMessage := &message.NatsStreamMessage{
		fakeRawMessage,
		fakeRawConnect,
		map[string]string{"protocol_type": "sub"},
	}

	cases := []struct {
		name         string
		propertyName string
		propertyVal  string
		msg          []byte
		ack          []byte
	}{
		{
			"set_property",
			"protocol_type",
			"sub",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_property",
			"protocol_type",
			"reply",
			[]byte(""),
			[]byte(""),
		},
		{
			"get_property",
			"random_string",
			"never_returned",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_message",
			"protocol_type",
			"sub",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"ack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"nack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("ERR"),
		},
	}

	for _, c := range cases {
		switch c.name {
		case "set_property":
			fakeMessage.SetProperty(c.propertyName, c.propertyVal)
			if val, ok := fakeMessage.GetProperty(c.propertyName); !ok || val != c.propertyVal {
				t.Error("Set Property failed to set approprate value")
			}
		case "get_property":
			if val, ok := fakeMessage.GetProperty(c.propertyName); ok && val == c.propertyVal {
				t.Error("Got Unset Property.")
			}
		case "set_message":
			fakeRawMessage.On("SetBody", c.msg).Return(mocks.RawMessage{})
			fakeRawMessage.On("GetBody").Return(c.msg)
			_ = fakeMessage.SetMessage(c.msg)
			if string(fakeMessage.GetMessage()) != string(c.msg) {
				t.Error("Failed to Set Message body")
			}
		case "ack":
			fakeRawConnect.On("Publish", c.propertyVal, c.ack).Return(nil).Once()
			fakeRawMessage.On("GetReplyTo").Return(c.propertyVal).Once()
			fakeRawMessage.On("Ack", false).Return(nil).Once()
			fakeMessage.SendAck(c.ack)
		case "nack":
			fakeRawConnect.On("Publish", c.propertyVal, c.ack).Return(nil).Once()
			fakeRawMessage.On("GetReplyTo").Return(c.propertyVal).Once()
			fakeMessage.SendNack(c.ack)
		default:
			t.Error("Unknown case")
		}
	}

}

func TestNatsStreamRawWrapper(t *testing.T) {

	wrapMessage := message.NatsStreamRawMessage{&natsStream.Msg{}}
	wrapMessage.GetBody()
	wrapMessage.SetBody([]byte(""))
	wrapMessage.GetReplyTo()
	wrapMessage.GetCorrelationId()

}

func TestRedisMessage(t *testing.T) {
	fakeRawMessage := &mocks.RawMessage{}
	fakeRawClient := &mocks.RawClient{}
	fakeMessage := &message.RedisMessage{
		fakeRawMessage,
		fakeRawClient,
		map[string]string{"protocol_type": "sub"},
	}

	cases := []struct {
		name         string
		propertyName string
		propertyVal  string
		msg          []byte
		ack          []byte
	}{
		{
			"set_property",
			"protocol_type",
			"sub",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_property",
			"protocol_type",
			"reply",
			[]byte(""),
			[]byte(""),
		},
		{
			"get_property",
			"random_string",
			"never_returned",
			[]byte(""),
			[]byte(""),
		},
		{
			"set_message",
			"protocol_type",
			"sub",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"ack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("OK"),
		},
		{
			"nack",
			"protocol_type",
			"reqrep",
			[]byte("MSG"),
			[]byte("ERR"),
		},
	}

	for _, c := range cases {
		switch c.name {
		case "set_property":
			fakeMessage.SetProperty(c.propertyName, c.propertyVal)
			if val, ok := fakeMessage.GetProperty(c.propertyName); !ok || val != c.propertyVal {
				t.Error("Set Property failed to set approprate value")
			}
		case "get_property":
			if val, ok := fakeMessage.GetProperty(c.propertyName); ok && val == c.propertyVal {
				t.Error("Got Unset Property.")
			}
		case "set_message":
			fakeRawMessage.On("SetBody", c.msg).Return(mocks.RawMessage{})
			fakeRawMessage.On("GetBody").Return(c.msg)
			_ = fakeMessage.SetMessage(c.msg)
			if string(fakeMessage.GetMessage()) != string(c.msg) {
				t.Error("Failed to Set Message body")
			}
		case "ack":
			fakeRawClient.On("Send", c.ack).Return(nil).Once()
			fakeMessage.SendAck(c.ack)
		case "nack":
			fakeRawClient.On("Send", c.ack).Return(nil).Once()
			fakeMessage.SendNack(c.ack)
		default:
			t.Error("Unknown case")
		}
	}

}

func TestRedisRawWrapper(t *testing.T) {

	wrapMessage := message.RedisRawMessage{}
	wrapMessage.Ack(false)
	wrapMessage.Nack(false, true)
}
