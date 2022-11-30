package message

type NanoMessage struct {
	RawMessage RawMessage
	Responder  RawSocket
	Properties map[string]string
}

func (m NanoMessage) GetProperty(key string) (string, bool) {
	if val, ok := m.Properties[key]; ok {
		return val, ok
	} else {
		return "", ok
	}
}

func (m NanoMessage) SetProperty(key string, val string) {
	m.Properties[key] = val
}

func (m NanoMessage) GetMessage() []byte {
	return m.RawMessage.GetBody()
}

func (m NanoMessage) SetMessage(msg []byte) Message {
	rawmsg := m.RawMessage.SetBody(msg)
	m.RawMessage = rawmsg
	return m
}

func (m NanoMessage) SendAck(ackMsg ...[]byte) {
	resp := []byte("OK")
	if len(ackMsg) > 0 {
		resp = ackMsg[0]
	}
	m.Responder.Send(resp)
	return
}

func (m NanoMessage) SendNack(ackMessage ...[]byte) {
	resp := []byte("ERR")
	if len(ackMessage) > 0 {
		resp = ackMessage[0]
	}
	m.Responder.Send(resp)
	return
}

func (m NanoMessage) SendAckWithError(ackMsg ...[]byte) error {
	resp := []byte("OK")
	if len(ackMsg) > 0 {
		resp = ackMsg[0]
	}
	return m.Responder.Send(resp)
}

func (m NanoMessage) SendNackWithError(ackMessage ...[]byte) error {
	resp := []byte("ERR")
	if len(ackMessage) > 0 {
		resp = ackMessage[0]
	}
	return m.Responder.Send(resp)
}
