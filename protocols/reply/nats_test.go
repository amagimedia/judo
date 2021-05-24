package reply

import (
	"errors"
	"testing"
	"time"

	"github.com/amagimedia/judo/v2/message"
	"github.com/amagimedia/judo/v2/message/mocks"
	nats "github.com/nats-io/go-nats"
	"github.com/stretchr/testify/mock"
)

func TestNatsReply(t *testing.T) {
	fakeConn := &mocks.RawConnection{}

	connector := func(url string) (message.RawConnection, error) {
		return fakeConn, nil
	}

	_ = func(url string) (message.RawConnection, error) {
		return fakeConn, errors.New("Cannot Create connection, Server not found")
	}

	fakeSubscriber := &NatsReply{connector: connector}

	cases := []struct {
		config  []interface{}
		retVal  string
		retType error
	}{
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
					"user":     "aaaa",
					"password": "asdf",
				},
			},
			"success-cfg-usr",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
					"token":    "aaaaFdsfwsnroidjf",
				},
			},
			"success-cfg-tok",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
				},
			},
			"success-cfg-noauth",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":  "dqi50n_agent",
					"topic": "dqi50n.out",
				},
			},
			"err-cfg",
			errors.New("Key Missing : endpoint"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
				},
			},
			"success-start",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
				},
			},
			"err-start",
			errors.New("Could not create subscription"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
				},
			},
			"err-conn",
			errors.New("Disconnected from nats server for dqi50n_agent"),
		},
	}
	for _, c := range cases {
		switch c.retVal {
		case "success-cfg-usr":
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
			_ = NewNatsReply()
		case "success-cfg-tok":
			fakeSubscriber = &NatsReply{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
		case "success-cfg-noauth":
			fakeSubscriber = &NatsReply{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
		case "err-cfg":
			fakeSubscriber = &NatsReply{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err.Error() != c.retType.Error() {
				t.Error("Error Expected, but did not occur")
			}
		case "success-start":
			fakeSubscriber = &NatsReply{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error Unexpected" + err.Error())
			}
			fakeConn.On("ChanSubscribe", "dqi50n.out", mock.Anything).Return(&nats.Subscription{}, nil).Once()
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("UnExpected Error")
			}
		case "err-start":
			fakeSubscriber = &NatsReply{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error Unexpected" + err.Error())
			}
			fakeConn.On("ChanSubscribe", "dqi50n.out", mock.Anything).Return(&nats.Subscription{}, c.retType).Once()
			_, err = fakeSubscriber.Start()
			if err.Error() != c.retType.Error() {
				t.Error("UnExpected Type of Error")
			}
		case "err-conn":
			ch := make(chan *nats.Msg)
			rcv := func() <-chan *nats.Msg { return ch }
			fakeSubscriber = &NatsReply{connector: connector, msgQueue: rcv()}
			called := false
			fakeSubscriber.OnMessage(func(message.Message) {
				called = true
			})
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error Unexpected" + err.Error())
			}
			fakeConn.On("ChanSubscribe", "dqi50n.out", mock.Anything).Return(&nats.Subscription{}, nil).Once()
			ec, err := fakeSubscriber.Start()
			go func() {
				ch <- &nats.Msg{}
				time.Sleep(time.Millisecond * 100)
				close(ch)
			}()
			err = <-ec
			if err.Error() != c.retType.Error() {
				t.Error("Unexpected Type of Error" + err.Error())
			}
			if !called {
				t.Error("Did not call callback")
			}
			fakeConn.On("Close").Return(nil)
			fakeSubscriber.Close()
		}
	}
}
