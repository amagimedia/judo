package client

import (
	jmsg "github.com/amagimedia/judo/v2/message"
	gredis "github.com/go-redis/redis"
)

type JudoClient interface {
	Configure([]interface{}) error
	OnMessage(func(msg jmsg.Message)) JudoClient
	Start() (<-chan error, error)
	Close()
	SetDependencies(*gredis.Client)
}
