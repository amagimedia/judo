package sub

import (
	"errors"
	"testing"
	"time"

	"github.com/amagimedia/judo/v3/config"
	"github.com/amagimedia/judo/v3/message"
	"github.com/amagimedia/judo/v3/message/mocks"
	stan "github.com/nats-io/go-nats-streaming"
	"github.com/stretchr/testify/mock"
)

func TestNatsStreamSubscriber(t *testing.T) {
	fakeConn := &mocks.RawConnection{}

	connector := func(url string, c config.Config, h func(stan.Conn, error)) (message.RawConnection, error) {
		return fakeConn, nil
	}

	_ = func(url string, c config.Config, h func(stan.Conn, error)) (message.RawConnection, error) {
		return fakeConn, errors.New("Cannot Create connection, Server not found")
	}

	fakeSubscriber := &NatsStreamSubscriber{connector: connector}

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
					"cluster":  "test",
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
					"cluster":  "test",
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
					"cluster":  "test",
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
			"err-cfg",
			errors.New("Key Missing : cluster"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"cluster":  "test",
					"endpoint": "localhost:3234",
				},
			},
			"err-cfg",
			errors.New("Key Missing : topic"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"topic":    "dqi50n.out",
					"cluster":  "test",
					"endpoint": "localhost:3234",
				},
			},
			"err-cfg",
			errors.New("Key Missing : name"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":     "dqi50n_agent",
					"cluster":  "test",
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
					"cluster":  "test",
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
					"cluster":  "test",
					"topic":    "dqi50n.out",
					"endpoint": "localhost:3234",
				},
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
			_ = NewNatsStreamSub()
		case "success-cfg-tok":
			fakeSubscriber = &NatsStreamSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
		case "success-cfg-noauth":
			fakeSubscriber = &NatsStreamSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unexpected config failure")
			}
		case "err-cfg":
			fakeSubscriber = &NatsStreamSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err.Error() != c.retType.Error() {
				t.Error("Error Expected, but did not occur")
			}
		case "success-start":
			fakeSubscriber = &NatsStreamSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error Unexpected" + err.Error())
			}
			fakeConn.On("Subscribe", "dqi50n.out", mock.Anything, mock.AnythingOfType("stan.SubscriptionOption"), mock.AnythingOfType("stan.SubscriptionOption")).Return(&mocks.Subscription{}, nil).Once()
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("UnExpected Error")
			}
		case "err-start":
			fakeSubscriber = &NatsStreamSubscriber{connector: connector}
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error Unexpected" + err.Error())
			}
			fakeConn.On("Subscribe", "dqi50n.out", mock.Anything, mock.AnythingOfType("stan.SubscriptionOption"), mock.AnythingOfType("stan.SubscriptionOption")).Return(&mocks.Subscription{}, c.retType).Once()
			_, err = fakeSubscriber.Start()
			if err.Error() != c.retType.Error() {
				t.Error("UnExpected Type of Error")
			}
		case "err-conn":
			ec := make(chan error)
			fakeSubscriber = &NatsStreamSubscriber{connector: connector, errorChannel: ec}
			called := false
			fakeSubscriber.OnMessage(func(message.Message) {
				called = true
			})
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Error Unexpected" + err.Error())
			}
			fakeConn.On("Subscribe", "dqi50n.out", mock.Anything, mock.AnythingOfType("stan.SubscriptionOption"), mock.AnythingOfType("stan.SubscriptionOption")).Return(&mocks.Subscription{}, nil).Once()
			_, err = fakeSubscriber.Start()
			go func() {
				fakeSubscriber.receive(&stan.Msg{})
				time.Sleep(time.Millisecond * 100)
				fakeSubscriber.errHandler(&mocks.Conn{}, c.retType)
				//ec <- c.retType
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
