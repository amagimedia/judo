package judo

import (
	"errors"
	"fmt"

	"github.com/amagimedia/judo/v2/client"
	judoMsg "github.com/amagimedia/judo/v2/message"
	amagiPub "github.com/amagimedia/judo/v2/protocols/pub/amagipub"
	pubnubPub "github.com/amagimedia/judo/v2/protocols/pub/pubnub"
	redispub "github.com/amagimedia/judo/v2/protocols/pub/redis"
	sidekiqpub "github.com/amagimedia/judo/v2/protocols/pub/sidekiq"
	stanpub "github.com/amagimedia/judo/v2/protocols/pub/stan"
	judoReply "github.com/amagimedia/judo/v2/protocols/reply"
	nanoreq "github.com/amagimedia/judo/v2/protocols/req/nano"
	judoSub "github.com/amagimedia/judo/v2/protocols/sub"
	"github.com/amagimedia/judo/v2/publisher"
)

func NewSubscriber(protocol, method, primarySub, backupSub string) (client.JudoClient, error) {

	var sub client.JudoClient
	switch protocol {
	case "amqp":
		switch method {
		case "sub":
			sub = judoSub.NewAmqpSub()
		case "reply":
			sub = judoReply.NewAmqpReply()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "nano":
		switch method {
		case "sub":
			sub = judoSub.NewNanoSub()
		case "reply":
			sub = judoReply.NewNanoReply()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "nats":
		switch method {
		case "sub":
			sub = judoSub.NewNatsSub()
		case "reply":
			sub = judoReply.NewNatsReply()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "nats-streaming":
		switch method {
		case "sub":
			sub = judoSub.NewNatsStreamSub()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "redis":
		switch method {
		case "sub":
			sub = judoSub.NewRedisSub()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "pubnub":
		switch method {
		case "sub":
			sub = judoSub.NewPubnubSub()
		default:
			return sub, errors.New("Invalid Parameters, method: " + method)
		}
	case "amagi":
		switch method {
		case "sub":
			sub = judoSub.NewAmagiSub(primarySub, backupSub)
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

func NewPublisher(pubType string, pubMethod string, primary string, backup string) (publisher.JudoPub, error) {
	// Switch on PubType
	// Pass Config to Connect and get Publisher Object
	// Return Publisher Object

	var pub publisher.JudoPub
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
	case "amagi-publish":
		pub, err = amagiPub.New(primary, backup)
		if err != nil {
			return pub, err
		}
	default:
		return nil, nil
	}
	return pub, err
}
