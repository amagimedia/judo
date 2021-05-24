package pub

import (
	pubnubPub "github.com/amagimedia/judo/v2/protocols/pub/pubnub"
	redispub "github.com/amagimedia/judo/v2/protocols/pub/redis"
	sidekiqpub "github.com/amagimedia/judo/v2/protocols/pub/sidekiq"
	stanpub "github.com/amagimedia/judo/v2/protocols/pub/stan"
	nanoreq "github.com/amagimedia/judo/v2/protocols/req/nano"
	"github.com/amagimedia/judo/v2/publisher"
)

type AmagiPub struct {
	primaryPub publisher.JudoPub
	backupPub  publisher.JudoPub
}

func New(primary, backup string) (publisher.JudoPub, error) {
	publishers := &AmagiPub{}
	var err error
	publishers.primaryPub, err = newPub(primary)
	if err != nil {
		return nil, err
	}
	publishers.backupPub, err = newPub(backup)
	if err != nil {
		return nil, err
	}
	return publishers, nil
}

func (publishers *AmagiPub) Connect(configs []interface{}) error {
	primaryConfig := []interface{}{configs[0]}
	err := publishers.primaryPub.Connect(primaryConfig)
	if err != nil {
		return err
	}
	backupConfig := []interface{}{configs[1]}
	err = publishers.backupPub.Connect(backupConfig)
	if err != nil {
		return err
	}
	return nil
}

func (publishers *AmagiPub) Publish(subject string, msg []byte) error {
	err := publishers.primaryPub.Publish(subject, msg)
	if err != nil {
		return err
	}
	if publishers.backupPub != nil {
		err = publishers.backupPub.Publish(subject, msg)
		return err
	}
	return nil
}

func (publishers *AmagiPub) Close() error {
	err := publishers.primaryPub.Close()
	if err != nil {
		return err
	}
	if publishers.backupPub != nil {
		err = publishers.backupPub.Close()
		return err
	}
	return nil
}

func newPub(protocol string) (publisher.JudoPub, error) {
	var pub publisher.JudoPub
	var err error
	switch protocol {
	case "redis":
		pub, err = redispub.New()
		if err != nil {
			return nil, err
		}
	case "sidekiq":
		pub, err = sidekiqpub.New()
		if err != nil {
			return nil, err
		}
	case "nano":
		pub, err = nanoreq.New()
		if err != nil {
			return nil, err
		}
	case "nats":
		pub, err = stanpub.New()
		if err != nil {
			return nil, err
		}
	case "pubnub":
		pub, err = pubnubPub.New()
		if err != nil {
			return nil, err
		}
	default:
		return pub, nil
	}
	return pub, nil
}
