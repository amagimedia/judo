package sub

import (
	"errors"
	"testing"
	"time"

	"github.com/amagimedia/judo/v2/message"
	"github.com/amagimedia/judo/v2/message/mocks"
	pubnub "github.com/pubnub/go"
	"github.com/stretchr/testify/mock"
)

func TestPubnubSubscriber(t *testing.T) {
	fakeClient := &mocks.PubnubRawClient{}

	connector := func(cfg pubnubConfig) (message.RawPubnubClient, error) {
		return fakeClient, nil
	}

	_ = func() (message.RawPubnubClient, error) {
		return fakeClient, errors.New("Cannot Create connection, Server not found")
	}

	fakeSubscriber := &PubnubSubscriber{connector: connector}

	cases := []struct {
		config  map[string]interface{}
		retVal  string
		retType error
	}{
		{
			map[string]interface{}{
				"name":          "dqi50n_agent",
				"topic":         "dqi50n.out",
				"subscribe_key": "demo",
				"persistence":   true,
			},
			"success-cfg",
			nil,
		},
		{
			map[string]interface{}{
				"name":        "dqi50n_agent",
				"topic":       "dqi50n.out",
				"persistence": true,
			},
			"error-cfg",
			errors.New("Key Missing : subscribe_key"),
		},
		{
			map[string]interface{}{
				"name":          "dqi50n_agent",
				"subscribe_key": "demo",
				"persistence":   true,
			},
			"error-cfg-1",
			errors.New("Key Missing : topic"),
		},
		{
			map[string]interface{}{
				"name":          "dqi50n_agent",
				"topic":         "dqi50n.out",
				"subscribe_key": "demo",
				"persistence":   true,
			},
			"success-start",
			nil,
		},
		{
			map[string]interface{}{
				"name":          "dqi50n_agent",
				"topic":         "dqi50n.out",
				"subscribe_key": "demo",
				"persistence":   true,
			},
			"dial-err",
			errors.New("Cannot dial to client"),
		},
		{
			map[string]interface{}{
				"name":          "dqi50n_agent",
				"topic":         "dqi50n.out",
				"subscribe_key": "demo",
				"persistence":   true,
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
				t.Error("Unable to Configure pubnub", err.Error())
			}
			_ = NewPubnubSub()
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
			ch := make(chan *pubnub.PNMessage)
			err := fakeSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			called := false
			fakeSubscriber.OnMessage(func(msg message.Message) {
				called = true
			})
			fakeClient.On("Subscribe", mock.AnythingOfType("string")).Return(nil)
			fakeClient.On("GetListeners").Return(map[*pubnub.Listener]bool{&pubnub.Listener{Message: ch}: true})
			fakeClient.On("FetchHistory", mock.AnythingOfType("string"), mock.AnythingOfType("bool"), mock.AnythingOfType("int64"), mock.AnythingOfType("bool"), mock.AnythingOfType("int")).Return(make([]*pubnub.PNMessage, 0))
			fakeClient.On("Destroy").Return(nil)
			_, err = fakeSubscriber.Start()
			if err != nil {
				t.Error("Failed in verifying start method.")
			}
			ch <- &pubnub.PNMessage{Message: map[string]interface{}{"msg": "Message_New"}, Timetoken: time.Now().UTC().UnixNano()}
			time.Sleep(time.Millisecond * 100)
			if !called {
				t.Error("Failed in verifying start method.")
			}
			fakeClient.On("Destroy").Return(nil)
			fakeSubscriber.Close()
		case "dial-err":
			connector := func(cfg pubnubConfig) (message.RawPubnubClient, error) {
				return fakeClient, c.retType
			}
			fSubscriber := &PubnubSubscriber{connector: connector}
			err := fSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}

			fakeSubscriber.OnMessage(func(message.Message) {
				return
			})
			fakeClient.On("Subscribe", mock.AnythingOfType("string")).Return(nil)
			fakeClient.On("FetchHistory", mock.AnythingOfType("string"), mock.AnythingOfType("bool"), mock.AnythingOfType("int64"), mock.AnythingOfType("bool"), mock.AnythingOfType("int")).Return(make([]*pubnub.PNMessage, 0))
			fakeClient.On("Destroy").Return(nil)
			_, err = fSubscriber.Start()
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		case "recv-err":
			fClient := &mocks.PubnubRawClient{}
			ch := make(chan *pubnub.PNMessage)
			cr := func(cfg pubnubConfig) (message.RawPubnubClient, error) {
				return fClient, nil
			}

			fSubscriber := &PubnubSubscriber{connector: cr}

			err := fSubscriber.Configure(c.config)
			if err != nil {
				t.Error("Configure failed when not expected.")
			}

			fSubscriber.OnMessage(func(message.Message) {
				return
			})
			fClient.On("Subscribe", mock.AnythingOfType("string")).Return(nil)
			fClient.On("GetListeners").Return(map[*pubnub.Listener]bool{&pubnub.Listener{Message: ch}: true})
			fClient.On("FetchHistory", mock.AnythingOfType("string"), mock.AnythingOfType("bool"), mock.AnythingOfType("int64"), mock.AnythingOfType("bool"), mock.AnythingOfType("int")).Return(make([]*pubnub.PNMessage, 0))
			fClient.On("Destroy").Return(nil)
			ec, err := fSubscriber.Start()
			close(ch)
			err = <-ec
			if err.Error() != c.retType.Error() {
				t.Error("Start did not fail when expected")
			}
		}
	}

}
