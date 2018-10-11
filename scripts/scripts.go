package scripts

const (
	XPUBLISHSHA   = "19139a75c1a4ac8df8887010e7e4d19033fcc747"
	XSUBSCRIBESHA = "7a888fa9085114a6f4a8f3074cda494b564eb494"
)

var SHAtoCode = map[string]string{
	XPUBLISHSHA:   "local topic = ARGV[1];local ts = ARGV[3];local msg = ts .. '|' .. ARGV[2];redis.call('PUBLISH', topic, msg);redis.call('ZADD', topic, ts, msg);return msg;",
	XSUBSCRIBESHA: "local topic = ARGV[1];local from = ARGV[2];local to = ARGV[3];return redis.call('ZRANGEBYSCORE', topic, from, to);",
}
