package service

import (
	"strconv"

	gredis "github.com/go-redis/redis"
)

type Duplicate struct {
	RedisConn *gredis.Client
	EventID   string
	TimeStamp int64
}

func (d Duplicate) IsDuplicate() bool {
	if d.RedisConn == nil {
		return false
	}
	topic := getHashName()
	if d.RedisConn.HExists(topic, d.EventID).Val() {
		timeStampString := d.RedisConn.HGet(topic, d.EventID).Val()
		timeStamp, err := strconv.ParseInt(timeStampString, 10, 0)
		if err != nil {
			panic(err)
		}
		if timeStamp >= d.TimeStamp {
			return true
		}
	}
	if err := d.RedisConn.HMSet(topic, map[string]interface{}{d.EventID: d.TimeStamp}).Err(); err != nil {
		panic(err)
	}
	return false
}

func getHashName() string {
	hashName := "duplicateEntryCheck"
	return hashName
}
