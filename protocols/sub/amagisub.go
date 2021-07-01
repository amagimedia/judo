package sub

import (
	"sync"

	"github.com/amagimedia/judo/v3/client"
	jmsg "github.com/amagimedia/judo/v3/message"
)

type AmagiSubscriber struct {
	primarySubscriber client.JudoClient
	backupSubscriber  client.JudoClient
}

func NewAmagiSub(primarySubProtocol, backupSubProtocol string) *AmagiSubscriber {
	subs := &AmagiSubscriber{}
	subs.primarySubscriber = newSub(primarySubProtocol)
	subs.backupSubscriber = newSub(backupSubProtocol)
	return subs
}

func (subs *AmagiSubscriber) Configure(config []interface{}) error {
	primaryConfig := []interface{}{config[0], config[2]}
	err := subs.primarySubscriber.Configure(primaryConfig)
	if err != nil {
		return err
	}
	backupConfig := []interface{}{config[1], config[2]}
	err = subs.backupSubscriber.Configure(backupConfig)
	if err != nil {
		return err
	}
	return err
}

func (subs *AmagiSubscriber) Close() {
	subs.primarySubscriber.Close()
	subs.backupSubscriber.Close()
}

func (subs *AmagiSubscriber) Start() (<-chan error, error) {
	combinedErrorChannel := make(chan error)
	errorChannel, err := subs.primarySubscriber.Start()
	if err != nil {
		return combinedErrorChannel, err
	}
	var mu sync.Mutex
	go func(errorChannel <-chan error) {
		mu.Lock()
		defer mu.Unlock()
		cherr := <-errorChannel
		combinedErrorChannel <- cherr
	}(errorChannel)

	backupErrorChannel, err := subs.backupSubscriber.Start()
	go func(backupErrorChannel <-chan error) {
		mu.Lock()
		defer mu.Unlock()
		cherr := <-backupErrorChannel
		combinedErrorChannel <- cherr
	}(backupErrorChannel)
	return combinedErrorChannel, err
}

func (subs *AmagiSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	subs.primarySubscriber.OnMessage(callback)
	subs.backupSubscriber.OnMessage(callback)
	return subs
}

func newSub(protocol string) client.JudoClient {
	var sub client.JudoClient
	switch protocol {
	case "amqp":
		sub = NewAmqpSub()
	case "nano":
		sub = NewNanoSub()
	case "nats":
		sub = NewNatsSub()
	case "nats-streaming":
		sub = NewNatsStreamSub()
	case "redis":
		sub = NewRedisSub()
	case "pubnub":
		sub = NewPubnubSub()
	}
	return sub
}
