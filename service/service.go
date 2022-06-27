package service

import (
	"os"

	gredis "github.com/go-redis/redis"
)

type Duplicate struct {
	UniqueID string
}

var redisConn *gredis.Client

func (d Duplicate) IsDuplicate() bool {
	// Ignore duplicates if REDIS is not set in environment varibales
	if _, ok := os.LookupEnv("CPLIVE_REDIS_URL"); !ok {
		return false
	}
	var redisConn = gredis.NewClient(&gredis.Options{
		Addr:     os.Getenv("CPLIVE_REDIS_URL"),
		Password: os.Getenv("CPLIVE_REDIS_PASSWORD"),
	})

	topic := getSetName()
	if redisConn.SIsMember(topic, d.UniqueID).Val() {
		redisConn.SRem(topic, d.UniqueID)
		return true
	}
	redisConn.SAdd(topic, d.UniqueID)
	return false
}

func getSetName() string {
	setName := "duplicateEntryCheck"
	return setName
}
