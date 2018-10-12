package scripts

const (
	XPUBLISHSHA   = "abdd73e097fb2b8ee41723e654806b29acaecc84"
	XSUBSCRIBESHA = "50cce0d2fec592ef7d1aa91fe17b71b26791cde3"
)

var SHAtoCode = map[string]string{
	XPUBLISHSHA:   "local topic = ARGV[1];local ts = redis.call(incrby,KEYS[2],1);local msg = ts .. '|' .. ARGV[2];redis.call('PUBLISH', topic, msg);redis.call('ZADD', KEYS[1], ts, msg);return msg;",
	XSUBSCRIBESHA: "local topic = ARGV[1];local from = '(' .. ARGV[2];return redis.call('ZRANGEBYSCORE', KEYS[1], from, '+inf');",
}
