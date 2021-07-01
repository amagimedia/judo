package judo

import (
	"reflect"
	"testing"
)

func TestCreateSubscriberFailure(t *testing.T) {
	cases := []struct {
		protocol string
		method   string
	}{
		{
			"amqp",
			"oooo",
		}, {
			"nano",
			"oooo",
		}, {
			"nats",
			"oooo",
		}, {
			"nats-streaming",
			"oooo",
		}, {
			"oooo",
			"oooo",
		},
	}

	for _, c := range cases {
		_, err := NewSubscriber(c.protocol, c.method, "", "")
		if err == nil {
			t.Errorf("Error Should be thrown for invalid protocol and methods: ")
		}
	}
}

func TestCreateSubscriberSuccess(t *testing.T) {
	cases := []struct {
		protocol string
		method   string
	}{
		{
			"amqp",
			"sub",
		}, {
			"amqp",
			"reply",
		}, {
			"nano",
			"sub",
		}, {
			"nano",
			"reply",
		}, {
			"nats",
			"sub",
		}, {
			"nats",
			"reply",
		},
		{
			"nats-streaming",
			"sub",
		},
		{
			"redis",
			"sub",
		},
		{
			"pubnub",
			"sub",
		},
	}

	for _, c := range cases {
		retVal, err := NewSubscriber(c.protocol, c.method, "", "")
		if err != nil {
			t.Error("Unknown error while creating subscriber: ", err.Error())
		}
		retType := reflect.TypeOf(retVal)
		switch retType.String() {
		case "*sub.AmqpSubscriber":
			if c.protocol != "amqp" && c.method != "sub" {
				t.Fail()
			}
		case "*sub.NanoSubscriber":
			if c.protocol != "nano" && c.method != "sub" {
				t.Fail()
			}
		case "*sub.NatsSubscriber":
			if c.protocol != "nats" && c.method != "sub" {
				t.Fail()
			}
		case "*reply.AmqpReply":
			if c.protocol != "amqp" && c.method != "reply" {
				t.Fail()
			}
		case "*reply.NanoReply":
			if c.protocol != "nano" && c.method != "reply" {
				t.Fail()
			}
		case "*reply.NatsReply":
			if c.protocol != "nats" && c.method != "reply" {
				t.Fail()
			}
		case "*sub.NatsStreamSubscriber":
			if c.protocol != "nats-streaming" && c.method != "sub" {
				t.Fail()
			}
		case "*sub.RedisSubscriber":
			if c.protocol != "redis" && c.method != "sub" {
				t.Fail()
			}
		case "*sub.PubnubSubscriber":
			if c.protocol != "pubnub" && c.method != "sub" {
				t.Fail()
			}
		default:
			t.Errorf("Unknown type returned : %s", retType.Name())
		}
	}
}

func TestCreatePublisherSuccess(t *testing.T) {
	cases := []struct {
		protocol string
		method   string
	}{
		{
			"nano",
			"req",
		},
		{
			"redis",
			"publish",
		},
	}

	for _, c := range cases {
		retVal, err := NewPublisher(c.protocol, c.method, "", "")
		if err != nil {
			t.Error("Unknown error while creating subscriber: ", err.Error())
		}
		retType := reflect.TypeOf(retVal)
		switch retType.String() {
		case "*nano.nanoReq":
			if c.method != "req" && c.protocol != "nano" {
				t.Error("Invalid type returned")
			}
		case "*redis.redisPub":
			if c.method != "publish" && c.protocol != "redis" {
				t.Error("Invalid type returned")
			}
		}
	}
}
