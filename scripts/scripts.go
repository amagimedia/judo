package scripts

const (
	XPUBLISHSHA   = "19139a75c1a4ac8df8887010e7e4d19033fcc747"
	XSUBSCRIBESHA = "bafcf505307208399d76636af3b73f57b75b7407"
)

var SHAtoCode = map[string]string{
	XPUBLISHSHA:   "local topic = ARGV[1];local ts = ARGV[3];local msg = ts .. '|' .. ARGV[2];redis.call('PUBLISH', topic, msg);redis.call('ZADD', topic, ts, msg);return msg;",
	XSUBSCRIBESHA: "local topic = ARGV[1];local from = '(' .. ARGV[2];return redis.call('ZRANGEBYSCORE', topic, from, '+inf');",
}
