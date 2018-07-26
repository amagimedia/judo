package sub

import (
	"errors"
	"github.com/amagimedia/judo/message"
	"github.com/amagimedia/judo/message/mocks"
	nats "github.com/nats-io/go-nats"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestNatsSubscriber(t *testing.T) {
	fakeConn := &mocks.RawConnection{}

	connector := func(url string) (message.RawConnection, error) {
		return fakeConn, nil
	}

	_ = func(url string) (message.RawConnection, error) {
		return fakeConn, errors.New("Cannot Create connection, Server not found")
	}

	fakeSubscriber := &NatsSubscriber{connector: connector}

	cases := []struct {
		config  map[string]interface{}
		retVal  string
		retType error
	}{
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "localhost:3234",
				"user":     "aaaa",
				"password": "asdf",
			},
			"success-cfg-usr",
			nil,
		},
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "localhost:3234",
				"token":    "aaaaFdsfwsnroidjf",
			},
			"success-cfg-tok",
			nil,
		},
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "localhost:3234",
			},
			"success-cfg-noauth",
			nil,
		},
		{
			map[string]interface{}{
				"name":  "dqi50n_agent",
				"topic": "dqi50n.out",
			},
			"err-cfg",
			errors.New("Key Missing : endpoint"),
		},
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "localhost:3234",
			},
			"success-start",
			nil,
		},
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "localhost:3234",
			},
			"err-start",
			errors.New("Could not create subscription"),
		},
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": "localhost:3234",
			},
			"err-conn",
			errors.New("Disconnected, from server for dqi50n_agent"),
		},
	}
	for _, c := range cases {
		switch c.retVal {
		case "success-cfg-usr":
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
			_ = NewNatsSub()
		case "success-cfg-tok":
			fakeSubscriber = &NatsSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
		case "success-cfg-noauth":
			fakeSubscriber = &NatsSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
		case "err-cfg":
			fakeSubscriber = &NatsSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err.Error() != c.retType.Error() {
				t.Error("Error Expected, but did not occur")
			}
		case "success-start":
			fakeSubscriber = &NatsSubscriber{connector: connector}
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
			fakeSubscriber = &NatsSubscriber{connector: connector}
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
			fakeSubscriber = &NatsSubscriber{connector: connector, msgQueue: rcv()}
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
				t.Error("Unexpected Type of Error")
			}
			if !called {
				t.Error("Did not call callback")
			}
			fakeConn.On("Close").Return(nil)
			fakeSubscriber.Close()
		}
	}
}
