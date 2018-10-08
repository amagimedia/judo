package sub

import (
	"errors"
	"github.com/amagimedia/judo/message"
	"github.com/amagimedia/judo/message/mocks"
	gredis "github.com/go-redis/redis"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestRedisSubscriber(t *testing.T) {
	fakeClient := &mocks.RawClient{}

	connector := func(cfg redisConfig) (message.RawClient, error) {
		return fakeClient, nil
	}

	_ = func() (message.RawClient, error) {
		return fakeClient, errors.New("Cannot Create connection, Server not found")
	}

	fakeSubscriber := &RedisSubscriber{connector: connector}

	cases := []struct {
		config  map[string]interface{}
		retVal  string
		retType error
	}{
		{
			map[string]interface{}{
				"name":     "dqi50n_agent",
				"topic":    "dqi50n.out",
				"endpoint": ":6379",
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
				"endpoint":  ":6379",
			},
			"error-cfg-1",
			errors.New("Key Missing : topic"),
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  ":6379",
			},
			"success-start",
			nil,
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  ":6379",
			},
			"dial-err",
			errors.New("Cannot dial to client"),
		},
		{
			map[string]interface{}{
				"name":      "dqi50n_agent",
				"topic":     "dqi50n.out",
				"separator": "|",
				"endpoint":  ":6379",
			},
			"recv-err",
			errors.New("Receive channel closed, Subscription ended."),
		},
	}
	for _, c := range cases {
		switch c.retVal {
		case "success-cfg":
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Unable to Configure redis", err.Error())
			}
			_ = NewRedisSub()
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
			ch := make(chan *gredis.Message)
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			called := false
			fakeSubscriber.OnMessage(func(message.Message) {
				called = true
			})
			fakeClient.On("Subscribe", mock.AnythingOfType("string")).Return(&gredis.PubSub{})
			fakeClient.On("Channel").Return(func() <-chan *gredis.Message { return ch })
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			ch <- &gredis.Message{Payload: "Message"}
			time.Sleep(time.Millisecond * 100)
			if !called {
				t.Error("Failed in verifying start method.")
			}
			fakeClient.On("Close").Return(nil)
			fakeSubscriber.Close()
		case "dial-err":
			connector := func(cfg redisConfig) (message.RawClient, error) {
				return fakeClient, c.retType
			}
			fSubscriber := &RedisSubscriber{connector: connector}
			err := fSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}

			fakeClient.On("Subscribe", mock.AnythingOfType("string")).Return(&gredis.PubSub{})
			fakeClient.On("Channel").Return(make(<-chan *gredis.Message))
			_, err = fSubscriber.Start()
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		case "recv-err":
			fClient := &mocks.RawClient{}
			ch := make(chan *gredis.Message)
			cr := func(cfg redisConfig) (message.RawClient, error) {
				return fClient, nil
			}

			fSubscriber := &RedisSubscriber{connector: cr}

			err := fSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}
			rch := func() <-chan *gredis.Message { return ch }()
			fClient.On("Subscribe", mock.AnythingOfType("string")).Return(&gredis.PubSub{})
			fClient.On("Channel").Return(rch)
			ec, err := fSubscriber.Start()
			close(ch)
			err = <-ec
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		}
	}

}
