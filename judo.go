package judo

import (
	"errors"
	"fmt"
	"gitlab.com/ajithnn/judo/client"
	judoMsg "gitlab.com/ajithnn/judo/message"
	judoReply "gitlab.com/ajithnn/judo/protocols/reply"
	judoSub "gitlab.com/ajithnn/judo/protocols/sub"
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
