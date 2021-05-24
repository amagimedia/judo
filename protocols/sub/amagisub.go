package sub

import (
	"sync"

	"github.com/amagimedia/judo/v2/client"
	jmsg "github.com/amagimedia/judo/v2/message"
)

type AmagiSub struct {
	primarySub client.JudoClient
	backupSub  client.JudoClient
}

func NewAmagiSub(primarySub, backupSub string) *AmagiSub {
	subs := &AmagiSub{}
	subs.primarySub = newSub(primarySub)
	subs.backupSub = newSub(backupSub)
	return subs
}

func (subs *AmagiSub) Configure(config []interface{}) error {
	primaryConfig := []interface{}{config[0]}
	err := subs.primarySub.Configure(primaryConfig)
	if err != nil {
		return err
	}
	backupConfig := []interface{}{config[1]}
	err = subs.backupSub.Configure(backupConfig)
	if err != nil {
		return err
	}
	return err
}

func (subs *AmagiSub) Close() {
	subs.primarySub.Close()
	subs.backupSub.Close()
}

func (subs *AmagiSub) Start() (<-chan error, error) {
	combinedErrorChannel := make(chan error)
	errorChannel, err := subs.primarySub.Start()
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

	backupErrorChannel, err := subs.backupSub.Start()
	go func(backupErrorChannel <-chan error) {
		mu.Lock()
		defer mu.Unlock()
		cherr := <-backupErrorChannel
		combinedErrorChannel <- cherr
	}(backupErrorChannel)
	return combinedErrorChannel, err
}

func (subs *AmagiSub) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	subs.primarySub.OnMessage(callback)
	subs.backupSub.OnMessage(callback)
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
