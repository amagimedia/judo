package client

import (
	jmsg "github.com/amagimedia/judo/message"
)

type JudoClient interface {
	Configure(map[string]interface{}) error
	OnMessage(func(msg jmsg.Message)) JudoClient
	Start() (<-chan error, error)
	Close()
}
