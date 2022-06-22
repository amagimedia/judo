package service

import (
	gredis "github.com/go-redis/redis"
)

type Duplicate struct {
	RedisConn *gredis.Client
	UniqueID  string
}

func (d Duplicate) IsDuplicate() bool {
	if d.RedisConn == nil {
		return false
	}
	topic := getSetName()
	if d.RedisConn.SIsMember(topic, d.UniqueID).Val() {
		d.RedisConn.SRem(topic, d.UniqueID)
		return true
	}
	d.RedisConn.SAdd(topic, d.UniqueID)
	return false
}

func getSetName() string {
	setName := "duplicateEntryCheck"
	return setName
}
