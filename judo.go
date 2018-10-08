package judo

import (
	"errors"
	"fmt"
	"github.com/amagimedia/judo/client"
	judoMsg "github.com/amagimedia/judo/message"
	"github.com/amagimedia/judo/protocols/pub/redis"
	judoReply "github.com/amagimedia/judo/protocols/reply"
	nanoreq "github.com/amagimedia/judo/protocols/req/nano"
	judoSub "github.com/amagimedia/judo/protocols/sub"
	"github.com/amagimedia/judo/publisher"
)

func NewSubscriber(protocol, method string) (client.JudoClient, error) {

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
	var err error

	switch pubType + "-" + pubMethod {
	case "redis-publish":
		pub, err = redis.New()
		if err != nil {
			return pub, err
		}
	case "nano-req":
		pub, err = nanoreq.New()
		if err != nil {
			return pub, err
		}
		/*
			case "nats":
				pub, err = nats.New()
				if err != nil {
					return pub, err
				}
		*/
	default:
		return nil, nil
	}
	return pub, nil
}
