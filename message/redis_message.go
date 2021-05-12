package message

type RedisMessage struct {
	RawMessage RawMessage
	Responder  RawClient
	Properties map[string]string
}

func (m *RedisMessage) GetProperty(key string) (string, bool) {
	if val, ok := m.Properties[key]; ok {
		return val, ok
	} else {
		return "", ok
	}
}

func (m *RedisMessage) SetProperty(key string, val string) {
	m.Properties[key] = val
}

func (m *RedisMessage) GetMessage() []byte {
	return m.RawMessage.GetBody()
}

func (m *RedisMessage) SetMessage(msg []byte) Message {
	rawmsg := m.RawMessage.SetBody(msg)
	m.RawMessage = rawmsg
	return m
}

func (m *RedisMessage) SendAck(ackMsg ...[]byte) {
	m.SetProperty("ack", "OK")
	return
}

func (m *RedisMessage) SendNack(ackMessage ...[]byte) {
	m.SetProperty("ack", "NOK")
	return
}

func (m *RedisMessage) IsDupliacteEntry() bool {
	if val, ok := m.GetProperty("uniqueID"); ok {
		return isDuplicateID(val)
	}
	return true
}
