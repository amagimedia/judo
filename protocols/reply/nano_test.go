package reply

import (
	"errors"
	"github.com/amagimedia/judo/v2/message"
	"github.com/amagimedia/judo/v2/message/mocks"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestNanoReply(t *testing.T) {
	fakeSocket := &mocks.RawSocket{}

	connector := func() (message.RawSocket, error) {
		return fakeSocket, nil
	}

	errorConnector := func() (message.RawSocket, error) {
		return fakeSocket, errors.New("Cannot Create connection, Server not found")
	}

	fakeSubscriber := &NanoReply{connector: connector}

	cases := []struct {
		config  map[string]interface{}
		retVal  string
		retType error
	}{
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "ipc:///tmp/dqi50n.out",
			},
			"success-cfg",
			nil,
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
			},
			"error-cfg",
			errors.New("Key Missing : endpoint"),
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"separator": "|",
				"endpoint":  "ipc:///tmp/dqi50n.out",
			},
			"error-cfg-1",
			errors.New("Key Missing : topic"),
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  "ipc:///tmp/dqi50n.out",
			},
			"success-start",
			nil,
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  "ipc:///tmp/dqi50n.out",
			},
			"dial-err",
			errors.New("Cannot dial to socket"),
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  "ipc:///tmp/dqi50n.out",
			},
			"recv-err",
			errors.New("Cannot recv from socket"),
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  "ipc:///tmp/dqi50n.out",
			},
			"conn-err",
			errors.New("Cannot Create connection, Server not found"),
		},
	}
	for _, c := range cases {
		switch c.retVal {
		case "success-cfg":
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unable to Configure amqp", err.Error())
			}
			_ = NewNanoReply()
		case "error-cfg":
			err := fakeSubscriber.Configure(c.config)
			if err.Error() != c.retType.Error() {
				t.Error("Invalid Error thrown", err.Error())
			}
		case "error-cfg-1":
			err := fakeSubscriber.Configure(c.config)
			if err.Error() != c.retType.Error() {
				t.Error("Invalid Error thrown", err.Error())
			}
		case "success-start":
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			called := false
			fakeSubscriber.OnMessage(func(message.Message) {
				called = true
			})
			fakeSocket.On("AddTransport", mock.Anything).Return(nil)
			fakeSocket.On("Listen", "ipc:///tmp/dqi50n.out").Return(nil).Once()
			fakeSocket.On("SetOption", mock.Anything, []byte("dqi50n.out")).Return(nil).Once()
			fakeSocket.On("Recv").Return([]byte("a|b"), nil)
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			time.Sleep(time.Millisecond * 100)
			if !called {
				t.Error("Failed in verifying start method.")
			}
			fakeSocket.On("Close").Return(nil)
			fakeSubscriber.Close()
		case "dial-err":
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}
			fakeSocket.On("AddTransport", mock.Anything).Return(nil)
			fakeSocket.On("Listen", "ipc:///tmp/dqi50n.out").Return(c.retType).Once()
			_, err = fakeSubscriber.Start()
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		case "recv-err":
			fSocket := &mocks.RawSocket{}
			cr := func() (message.RawSocket, error) {
				return fSocket, nil
			}

			fSubscriber := &NanoReply{connector: cr}

			err := fSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}
			fSocket.On("AddTransport", mock.Anything).Return(nil)
			fSocket.On("Listen", "ipc:///tmp/dqi50n.out").Return(nil).Once()
			fSocket.On("SetOption", mock.Anything, []byte("dqi50n.out")).Return(nil).Once()
			fSocket.On("Recv").Return([]byte(""), c.retType).Once()
			ec, err := fSubscriber.Start()
			err = <-ec
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		case "conn-err":

			fSubscriber := &NanoReply{connector: errorConnector}

			err := fSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}

			_, err = fSubscriber.Start()

			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		}
	}

}
