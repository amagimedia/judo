package sub

import (
	"errors"
	"testing"
	"time"

	"github.com/amagimedia/judo/v3/message"
	"github.com/amagimedia/judo/v3/message/mocks"
	gredis "github.com/go-redis/redis"
	"github.com/stretchr/testify/mock"
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
		config  []interface{}
		retVal  string
		retType error
	}{
		{
			[]interface{}{
				map[string]interface{}{
					"name":        "dqi50n_agent",
					"topic":       "dqi50n.out",
					"endpoint":    ":6379",
					"persistence": true,
				},
			},
			"success-cfg",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":        "dqi50n_agent",
					"topic":       "dqi50n.out",
					"separator":   "|",
					"persistence": true,
				},
			},
			"error-cfg",
			errors.New("Key Missing : endpoint"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":        "dqi50n_agent",
					"separator":   "|",
					"endpoint":    ":6379",
					"persistence": true,
				},
			},
			"error-cfg-1",
			errors.New("Key Missing : topic"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":        "dqi50n_agent",
					"topic":       "dqi50n.out",
					"separator":   "|",
					"endpoint":    ":6379",
					"persistence": true,
				},
			},
			"success-start",
			nil,
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":        "dqi50n_agent",
					"topic":       "dqi50n.out",
					"separator":   "|",
					"persistence": true,
					"endpoint":    ":6379",
				},
			},
			"dial-err",
			errors.New("Cannot dial to client"),
		},
		{
			[]interface{}{
				map[string]interface{}{
					"name":        "dqi50n_agent",
					"topic":       "dqi50n.out",
					"separator":   "|",
					"endpoint":    ":6379",
					"persistence": true,
				},
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
			fakeSubscriber.OnMessage(func(msg message.Message) {
				called = true
			})
			fakeClient.On("Subscribe", mock.AnythingOfType("string")).Return(&gredis.PubSub{})
			fakeClient.On("Channel").Return(func() <-chan *gredis.Message { return ch })
			fakeClient.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(gredis.NewCmdResult(make([]interface{}, 0), nil))
			fakeClient.On("ScriptLoad", mock.AnythingOfType("string")).Return(&gredis.StringCmd{})
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			ch <- &gredis.Message{Payload: "Message_New"}
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

			fakeSubscriber.OnMessage(func(message.Message) {
				return
			})
			fakeClient.On("Subscribe", mock.AnythingOfType("string")).Return(&gredis.PubSub{})
			fakeClient.On("Channel").Return(make(<-chan *gredis.Message))
			fakeClient.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(gredis.NewCmdResult(make([]interface{}, 0), nil))
			fakeClient.On("ScriptLoad", mock.AnythingOfType("string")).Return(&gredis.StringCmd{})
			fakeClient.On("Close").Return(nil)
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

			fSubscriber.OnMessage(func(message.Message) {
				return
			})
			fClient.On("Subscribe", mock.AnythingOfType("string")).Return(&gredis.PubSub{})
			fClient.On("Channel").Return(rch)
			fClient.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(gredis.NewCmdResult([]interface{}{"aaaa"}, nil))
			fClient.On("ScriptLoad", mock.AnythingOfType("string")).Return(&gredis.StringCmd{})
			fClient.On("Close").Return(nil)
			ec, err := fSubscriber.Start()
			close(ch)
			err = <-ec
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		}
	}

}
