package message

type PubnubMessage struct {
	RawMessage RawMessage
	Responder  RawPubnubClient
	Properties map[string]string
}

func (m *PubnubMessage) GetProperty(key string) (string, bool) {
	if val, ok := m.Properties[key]; ok {
		return val, ok
	} else {
		return "", ok
	}
}

func (m *PubnubMessage) SetProperty(key string, val string) {
	m.Properties[key] = val
}

func (m *PubnubMessage) GetMessage() []byte {
	return m.RawMessage.GetBody()
}

func (m *PubnubMessage) SetMessage(msg []byte) Message {
	rawmsg := m.RawMessage.SetBody(msg)
	m.RawMessage = rawmsg
	return m
}

func (m *PubnubMessage) SendAck(ackMsg ...[]byte) {
	m.SetProperty("ack", "OK")
	return
}

func (m *PubnubMessage) SendNack(ackMessage ...[]byte) {
	m.SetProperty("ack", "NOK")
	return
}
