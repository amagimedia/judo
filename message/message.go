package message

import (
	gredis "github.com/go-redis/redis"
	nats "github.com/nats-io/go-nats"
	natsStream "github.com/nats-io/go-nats-streaming"
	"github.com/streadway/amqp"
	mangos "nanomsg.org/go-mangos"
)

type Message interface {
	GetMessage() []byte
	SetMessage([]byte) Message
	GetProperty(string) (string, bool)
	SetProperty(string, string)
	SendAck(...[]byte)
	SendNack(...[]byte)
}

type RawMessage interface {
	Ack(bool) error
	Nack(bool, bool) error
	GetBody() []byte
	SetBody([]byte) RawMessage
	GetReplyTo() string
	GetCorrelationId() string
}

type RawChannel interface {
	Publish(string, string, bool, bool, interface{}) error
	QueueBind(string, string, string, bool, interface{}) error
	QueueDeclare(string, bool, bool, bool, bool, interface{}) (amqp.Queue, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, interface{}) error
	Consume(string, string, bool, bool, bool, bool, interface{}) (<-chan amqp.Delivery, error)
	Close() error
	Qos(int, int, bool) error
}

type RawSocket interface {
	Recv() ([]byte, error)
	Close() error
	AddTransport(mangos.Transport)
	Dial(string) error
	Listen(string) error
	SetOption(string, interface{}) error
	Send([]byte) error
}

type RawConnection interface {
	Publish(string, []byte) error
	ChanSubscribe(string, interface{}) (*nats.Subscription, error)
	Close()
	Subscribe(string, natsStream.MsgHandler, ...natsStream.SubscriptionOption) (natsStream.Subscription, error)
}

type RawClient interface {
	Publish(string, interface{}) *gredis.IntCmd
	Subscribe(...string) RawClient
	Close() error
	Channel() <-chan *gredis.Message
}

type AmqpRawMessage struct {
	amqp.Delivery
}

func (d AmqpRawMessage) Ack(multiple bool) error {
	return d.Delivery.Ack(multiple)
}

func (d AmqpRawMessage) Nack(multiple, requeue bool) error {
	return d.Delivery.Nack(multiple, requeue)
}

func (d AmqpRawMessage) GetBody() []byte {
	return d.Delivery.Body
}

func (d AmqpRawMessage) SetBody(body []byte) RawMessage {
	d.Delivery.Body = body
	return d
}

func (d AmqpRawMessage) GetReplyTo() string {
	return d.Delivery.ReplyTo
}

func (d AmqpRawMessage) GetCorrelationId() string {
	return d.Delivery.CorrelationId
}

type AmqpRawChannel struct {
	*amqp.Channel
}

func (d AmqpRawChannel) Publish(exchange, key string, mandatory, immediate bool, msg interface{}) error {
	return d.Channel.Publish(exchange, key, mandatory, immediate, msg.(amqp.Publishing))
}

func (d AmqpRawChannel) QueueBind(name, key, exchange string, noWait bool, args interface{}) error {
	return d.Channel.QueueBind(name, key, exchange, noWait, args.(amqp.Table))
}

func (d AmqpRawChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args interface{}) (amqp.Queue, error) {
	return d.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args.(amqp.Table))
}

func (d AmqpRawChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args interface{}) error {
	return d.Channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args.(amqp.Table))
}

func (d AmqpRawChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args interface{}) (<-chan amqp.Delivery, error) {
	return d.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args.(amqp.Table))
}

func (d AmqpRawChannel) Close() error {
	if d.Channel != nil {
		return d.Channel.Close()
	}
	return nil
}

func (d AmqpRawChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return d.Channel.Qos(prefetchCount, prefetchSize, global)
}

type NanoRawMessage struct {
	Raw []byte
}

func (d NanoRawMessage) Ack(multiple bool) error {
	return nil
}

func (d NanoRawMessage) Nack(multiple, requeue bool) error {
	return nil
}

func (d NanoRawMessage) GetBody() []byte {
	return d.Raw
}

func (d NanoRawMessage) SetBody(body []byte) RawMessage {
	d.Raw = body
	return d
}

func (d NanoRawMessage) GetReplyTo() string {
	return ""
}

func (d NanoRawMessage) GetCorrelationId() string {
	return ""
}

type NanoRawSocket struct {
	mangos.Socket
}

func (d NanoRawSocket) Send(msg []byte) error {
	return d.Socket.Send(msg)
}

func (d NanoRawSocket) Recv() ([]byte, error) {
	return d.Socket.Recv()
}

func (d NanoRawSocket) Close() error {
	if d.Socket != nil {
		return d.Socket.Close()
	}
	return nil
}

func (d NanoRawSocket) AddTransport(t mangos.Transport) {
	d.Socket.AddTransport(t)
}

func (d NanoRawSocket) SetOption(key string, val interface{}) error {
	return d.Socket.SetOption(key, val)
}

func (d NanoRawSocket) Dial(addr string) error {
	return d.Socket.Dial(addr)
}

func (d NanoRawSocket) Listen(addr string) error {
	return d.Socket.Listen(addr)
}

type NatsRawMessage struct {
	*nats.Msg
}

func (d NatsRawMessage) Ack(multiple bool) error {
	return nil
}

func (d NatsRawMessage) Nack(multiple, requeue bool) error {
	return nil
}

func (d NatsRawMessage) GetBody() []byte {
	return d.Msg.Data
}

func (d NatsRawMessage) SetBody(body []byte) RawMessage {
	d.Msg.Data = body
	return d
}

func (d NatsRawMessage) GetReplyTo() string {
	return d.Msg.Reply
}

func (d NatsRawMessage) GetCorrelationId() string {
	return ""
}

type NatsRawConnection struct {
	nats.Conn
}

func (d NatsRawConnection) Publish(subject string, msg []byte) error {
	return d.Conn.Publish(subject, msg)
}

func (d NatsRawConnection) ChanSubscribe(subject string, ch interface{}) (*nats.Subscription, error) {
	return d.Conn.ChanSubscribe(subject, ch.(chan *nats.Msg))
}

func (d NatsRawConnection) Subscribe(subject string, cb natsStream.MsgHandler, opts ...natsStream.SubscriptionOption) (natsStream.Subscription, error) {
	c, _ := natsStream.Connect("", "", func(a *natsStream.Options) error { return nil })
	return c.Subscribe(subject, cb, opts...)
}

func (d NatsRawConnection) Close() {
	if d.Conn.IsConnected() {
		d.Conn.Close()
	}
}

type NatsStreamRawMessage struct {
	*natsStream.Msg
}

func (d NatsStreamRawMessage) Ack(multiple bool) error {
	return d.Msg.Ack()
}

func (d NatsStreamRawMessage) Nack(multiple, requeue bool) error {
	return nil
}

func (d NatsStreamRawMessage) GetBody() []byte {
	return d.Msg.Data
}

func (d NatsStreamRawMessage) SetBody(body []byte) RawMessage {
	d.Msg.Data = body
	return d
}

func (d NatsStreamRawMessage) GetReplyTo() string {
	return d.Msg.Reply
}

func (d NatsStreamRawMessage) GetCorrelationId() string {
	return ""
}

type NatsStreamRawConnection struct {
	natsStream.Conn
}

func (d NatsStreamRawConnection) Publish(subject string, msg []byte) error {
	return d.Conn.Publish(subject, msg)
}
func (d NatsStreamRawConnection) ChanSubscribe(subject string, ch interface{}) (*nats.Subscription, error) {
	return &nats.Subscription{}, nil
}

func (d NatsStreamRawConnection) Subscribe(subject string, cb natsStream.MsgHandler, opts ...natsStream.SubscriptionOption) (natsStream.Subscription, error) {
	return d.Conn.Subscribe(subject, cb, opts...)
}

func (d NatsStreamRawConnection) Close() {
	if d.Conn != nil {
		d.Conn.Close()
	}
}

type RedisRawMessage struct {
	Message *gredis.Message
}

func (d RedisRawMessage) Ack(multiple bool) error {
	return nil
}

func (d RedisRawMessage) Nack(multiple, requeue bool) error {
	return nil
}

func (d RedisRawMessage) GetBody() []byte {
	return []byte(d.Message.String())
}

func (d RedisRawMessage) SetBody(body []byte) RawMessage {
	d.Message.Payload = string(body)
	return d
}

func (d RedisRawMessage) GetReplyTo() string {
	return ""
}

func (d RedisRawMessage) GetCorrelationId() string {
	return ""
}

type RedisRawClient struct {
	Client *gredis.Client
	PubSub *gredis.PubSub
}

func (d RedisRawClient) Publish(subject string, msg interface{}) *gredis.IntCmd {
	return d.Client.Publish(subject, msg)
}

func (d RedisRawClient) Subscribe(channels ...string) RawClient {
	d.PubSub = d.Client.Subscribe(channels...)
	return d
}

func (d RedisRawClient) Channel() <-chan *gredis.Message {
	return d.PubSub.Channel()
}

func (d RedisRawClient) Close() error {
	if d.Client != nil {
		return d.Client.Close()
	}
	return nil
}
