package client

import (
	jmsg "gitlab.com/ajithnn/judo/message"
)

type JudoClient interface {
	Configure(map[string]interface{}) error
	OnMessage(func(msg jmsg.Message)) JudoClient
	Start() (<-chan error, error)
	Close()
}
