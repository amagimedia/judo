package client

import (
	jmsg "github.com/amagimedia/judo/v2/message"
)

type JudoClient interface {
	Configure([]interface{}) error
	OnMessage(func(msg jmsg.Message)) JudoClient
	Start() (<-chan error, error)
	Close()
}
