package message

type NatsMessage struct {
	RawMessage RawMessage
	Responder  RawConnection
	Properties map[string]string
}

func (m NatsMessage) GetProperty(key string) (string, bool) {
	if val, ok := m.Properties[key]; ok {
		return val, ok
	} else {
		return "", ok
	}
}

func (m NatsMessage) SetProperty(key string, val string) {
	m.Properties[key] = val
}

func (m NatsMessage) GetMessage() []byte {
	return m.RawMessage.GetBody()
}

func (m NatsMessage) SetMessage(msg []byte) Message {
	rawMsg := m.RawMessage.SetBody(msg)
	m.RawMessage = rawMsg
	return m
}

func (m NatsMessage) SendAck(ackMsg ...[]byte) {
	resp := []byte("OK")
	if len(ackMsg) > 0 {
		resp = ackMsg[0]
	}
	m.Responder.Publish(m.RawMessage.GetReplyTo(), resp)
	return
}

func (m NatsMessage) SendNack(ackMessage ...[]byte) {
	resp := []byte("NOK")
	if len(ackMessage) > 0 {
		resp = ackMessage[0]
	}
	m.Responder.Publish(m.RawMessage.GetReplyTo(), resp)
	return
}

func (m NatsMessage) IsDupliacteEntry() bool {
	if val, ok := m.GetProperty("uniqueID"); ok {
		return isDuplicateID(val)
	}
	return true
}
