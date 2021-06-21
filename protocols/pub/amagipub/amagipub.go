package pub

import (
	"fmt"

	pubnubPub "github.com/amagimedia/judo/v2/protocols/pub/pubnub"
	redispub "github.com/amagimedia/judo/v2/protocols/pub/redis"
	sidekiqpub "github.com/amagimedia/judo/v2/protocols/pub/sidekiq"
	stanpub "github.com/amagimedia/judo/v2/protocols/pub/stan"
	nanoreq "github.com/amagimedia/judo/v2/protocols/req/nano"
	"github.com/amagimedia/judo/v2/publisher"
	"github.com/google/uuid"
)

type AmagiPub struct {
	primaryPublisher publisher.JudoPub
	backupPublisher  publisher.JudoPub
}

func New(primaryPubProtocol, backupPubProtocol string) (publisher.JudoPub, error) {
	publishers := &AmagiPub{}
	var err error
	publishers.primaryPublisher, err = NewPublisher(primaryPubProtocol)
	if err != nil {
		return nil, err
	}
	publishers.backupPublisher, err = NewPublisher(backupPubProtocol)
	if err != nil {
		return nil, err
	}
	return publishers, nil
}

func (publishers *AmagiPub) Connect(configs []interface{}) error {
	primaryConfig := []interface{}{configs[0]}
	err := publishers.primaryPublisher.Connect(primaryConfig)
	if err != nil {
		return err
	}
	backupConfig := []interface{}{configs[1]}
	err = publishers.backupPublisher.Connect(backupConfig)
	if err != nil {
		return err
	}
	return nil
}

func (publishers *AmagiPub) Publish(subject string, msg []byte) error {
	dt, _ := uuid.NewRandom()
	msgString := fmt.Sprintf("%s|%s", dt.String(), string(msg))
	msgNew := []byte(msgString)
	err := publishers.primaryPublisher.Publish(subject, msgNew)
	if err != nil {
		return err
	}
	if publishers.backupPublisher != nil {
		err = publishers.backupPublisher.Publish(subject, msgNew)
		return err
	}
	return nil
}

func (publishers *AmagiPub) Close() error {
	var errs []error
	errs = append(errs, publishers.primaryPublisher.Close())
	if publishers.backupPublisher != nil {
		errs = append(errs, publishers.backupPublisher.Close())
	}
	if len(errs) != 0 {
		return errs[0]
	}
	return nil
}

func NewPublisher(protocol string) (publisher.JudoPub, error) {
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
