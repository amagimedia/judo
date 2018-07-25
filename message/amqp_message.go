package message

import (
	"github.com/streadway/amqp"
)

type AmqpMessage struct {
	RawMessage RawMessage
	Responder  RawChannel
	Properties map[string]string
}

func (m AmqpMessage) GetProperty(key string) (string, bool) {
	if val, ok := m.Properties[key]; ok {
		return val, ok
	} else {
		return "", ok
	}
}

func (m AmqpMessage) SetProperty(key string, val string) {
	m.Properties[key] = val
}

func (m AmqpMessage) GetMessage() []byte {
	return m.RawMessage.GetBody()
}

func (m AmqpMessage) SetMessage(msg []byte) Message {
	RawMsg := m.RawMessage.SetBody(msg)
	m.RawMessage = RawMsg
	return m
}

func (m AmqpMessage) SendAck(ackMessage ...[]byte) {
	if val, ok := m.GetProperty("protocol_type"); ok && val == "reqrep" {
		resp := []byte("OK")
		if len(ackMessage) > 0 {
			resp = ackMessage[0]
		}
		m.Responder.Publish(
			"",
			m.RawMessage.GetReplyTo(),
			false,
			false,
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: m.RawMessage.GetCorrelationId(),
				Body:          resp,
			},
		)
	}
	m.RawMessage.Ack(false)
}

func (m AmqpMessage) SendNack(ackMessage ...[]byte) {
	m.RawMessage.Nack(false, true)
}
