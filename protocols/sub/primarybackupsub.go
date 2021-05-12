package sub

import (
	"errors"
	"sync"

	"github.com/amagimedia/judo/v2/client"
	jmsg "github.com/amagimedia/judo/v2/message"
)

type PrimaryBackupSubscriber struct {
	primarySub client.JudoClient
	backupSub  client.JudoClient
}

func NewPrimaryBackupSub(primarySub client.JudoClient) *PrimaryBackupSubscriber {
	subs := &PrimaryBackupSubscriber{primarySub: primarySub}
	return subs
}

func (subs *PrimaryBackupSubscriber) Configure(config map[string]interface{}) error {
	err := subs.primarySub.Configure(config)
	if _, ok := config["backupsub"]; ok {
		backupData := config["backupsub"].(map[string]interface{})
		subs.newBackupSub(backupData["type"].(string))
		backupConfig := backupData["config"].(map[string]interface{})
		err = subs.backupSub.Configure(backupConfig)
		if err != nil {
			return err
		}
	}
	return err
}

func (subs *PrimaryBackupSubscriber) Close() {
	subs.primarySub.Close()
	subs.backupSub.Close()
}

func (subs *PrimaryBackupSubscriber) Start() (<-chan error, error) {
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

func (subs *PrimaryBackupSubscriber) OnMessage(callback func(msg jmsg.Message)) client.JudoClient {
	subs.primarySub.OnMessage(callback)
	if subs.backupSub != nil {
		subs.backupSub.OnMessage(callback)
	}
	return subs
}

func (subs *PrimaryBackupSubscriber) newBackupSub(protocol string) error {
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
	default:
		errors.New("Invalid Parameters")
	}
	subs.backupSub = sub
	return nil
}
