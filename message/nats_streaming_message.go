package message

type NatsStreamMessage struct {
	RawMessage RawMessage
	Responder  RawConnection
	Properties map[string]string
}

func (m NatsStreamMessage) GetProperty(key string) (string, bool) {
	if val, ok := m.Properties[key]; ok {
		return val, ok
	} else {
		return "", ok
	}
}

func (m NatsStreamMessage) SetProperty(key string, val string) {
	m.Properties[key] = val
}

func (m NatsStreamMessage) GetMessage() []byte {
	return m.RawMessage.GetBody()
}

func (m NatsStreamMessage) SetMessage(msg []byte) Message {
	rawMsg := m.RawMessage.SetBody(msg)
	m.RawMessage = rawMsg
	return m
}

func (m NatsStreamMessage) SendAck(ackMsg ...[]byte) {
	m.RawMessage.Ack(false)
	return
}

func (m NatsStreamMessage) SendNack(ackMessage ...[]byte) {
	return
}

func (m NatsStreamMessage) IsDupliacteEntry() bool {
	if val, ok := m.GetProperty("uniqueID"); ok {
		return isDuplicateID(val)
	}
	return true
}
