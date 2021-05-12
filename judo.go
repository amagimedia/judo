package judo

import (
	"errors"
	"fmt"

	"github.com/amagimedia/judo/v2/client"
	judoMsg "github.com/amagimedia/judo/v2/message"
	primaryBackupPub "github.com/amagimedia/judo/v2/protocols/pub/primarybackuppub"
	pubnubPub "github.com/amagimedia/judo/v2/protocols/pub/pubnub"
	redispub "github.com/amagimedia/judo/v2/protocols/pub/redis"
	sidekiqpub "github.com/amagimedia/judo/v2/protocols/pub/sidekiq"
	stanpub "github.com/amagimedia/judo/v2/protocols/pub/stan"
	judoReply "github.com/amagimedia/judo/v2/protocols/reply"
	nanoreq "github.com/amagimedia/judo/v2/protocols/req/nano"
	judoSub "github.com/amagimedia/judo/v2/protocols/sub"
	"github.com/amagimedia/judo/v2/publisher"
)

func NewSubscriber(protocol, method string) (client.JudoClient, error) {

	var sub client.JudoClient
	var primarySub client.JudoClient
	switch protocol {
	case "amqp":
		switch method {
		case "sub":
			primarySub = judoSub.NewAmqpSub()
			sub = judoSub.NewPrimaryBackupSub(primarySub)
		case "reply":
			sub = judoReply.NewAmqpReply()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "nano":
		switch method {
		case "sub":
			primarySub = judoSub.NewNanoSub()
			sub = judoSub.NewPrimaryBackupSub(primarySub)
		case "reply":
			sub = judoReply.NewNanoReply()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "nats":
		switch method {
		case "sub":
			primarySub = judoSub.NewNatsSub()
			sub = judoSub.NewPrimaryBackupSub(primarySub)
		case "reply":
			sub = judoReply.NewNatsReply()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "nats-streaming":
		switch method {
		case "sub":
			primarySub = judoSub.NewNatsStreamSub()
			sub = judoSub.NewPrimaryBackupSub(primarySub)
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "redis":
		switch method {
		case "sub":
			primarySub = judoSub.NewRedisSub()
			sub = judoSub.NewPrimaryBackupSub(primarySub)
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "pubnub":
		switch method {
		case "sub":
			primarySub := judoSub.NewPubnubSub()
			sub = judoSub.NewPrimaryBackupSub(primarySub)
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	default:
		return sub, errors.New("Invalid Protocol: " + protocol)
	}

	sub.OnMessage(func(msg judoMsg.Message) {
		fmt.Println("Received : ", msg.GetMessage())
	})

	return sub, nil
}

func NewPublisher(pubType string, pubMethod string) (publisher.JudoPub, error) {
	// Switch on PubType
	// Pass Config to Connect and get Publisher Object
	// Return Publisher Object

	var pub publisher.JudoPub
	var publishers publisher.JudoPub
	var err error

	switch pubType + "-" + pubMethod {
	case "redis-publish":
		pub, err = redispub.New()
		if err != nil {
			return pub, err
		}
	case "sidekiq-publish":
		pub, err = sidekiqpub.New()
		if err != nil {
			return pub, err
		}
	case "nano-req":
		pub, err = nanoreq.New()
		if err != nil {
			return pub, err
		}
	case "nats-publish":
		pub, err = stanpub.New()
		if err != nil {
			return pub, err
		}
	case "pubnub-publish":
		pub, err = pubnubPub.New()
		if err != nil {
			return pub, err
		}
	default:
		return nil, nil
	}
	publishers, err = primaryBackupPub.New(pub)
	return publishers, err
}
