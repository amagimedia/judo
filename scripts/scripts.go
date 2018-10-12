package scripts

const (
	XPUBLISHSHA   = "2a5ed96ba3d75dcf080ecfe06b25ae7db1f63abe"
	XSUBSCRIBESHA = "50cce0d2fec592ef7d1aa91fe17b71b26791cde3"
)

var SHAtoCode = map[string]string{
	XPUBLISHSHA:   "local topic = ARGV[1];local ts = redis.call('incr',KEYS[2]);local msg = ts .. '|' .. ARGV[2];redis.call('PUBLISH', topic, msg);redis.call('ZADD', KEYS[1], ts, msg);return msg;",
	XSUBSCRIBESHA: "local topic = ARGV[1];local from = '(' .. ARGV[2];return redis.call('ZRANGEBYSCORE', KEYS[1], from, '+inf');",
}
