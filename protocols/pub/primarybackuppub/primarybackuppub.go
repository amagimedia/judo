package pub

import (
	pubnubPub "github.com/amagimedia/judo/v2/protocols/pub/pubnub"
	redispub "github.com/amagimedia/judo/v2/protocols/pub/redis"
	sidekiqpub "github.com/amagimedia/judo/v2/protocols/pub/sidekiq"
	stanpub "github.com/amagimedia/judo/v2/protocols/pub/stan"
	nanoreq "github.com/amagimedia/judo/v2/protocols/req/nano"
	"github.com/amagimedia/judo/v2/publisher"
)

type PrimaryBackupPublisher struct {
	primaryPub publisher.JudoPub
	backupPub  publisher.JudoPub
}

func New(primaryPub publisher.JudoPub) (publisher.JudoPub, error) {
	publishers := &PrimaryBackupPublisher{primaryPub: primaryPub}
	return publishers, nil
}

func (publishers *PrimaryBackupPublisher) Connect(configs map[string]interface{}) error {
	err := publishers.primaryPub.Connect(configs)
	if err != nil {
		return err
	}
	if _, ok := configs["backup"]; ok {

		backupData := configs["backup"].(map[string]interface{})
		err := publishers.newBackupPub(backupData["type"].(string))
		if err != nil {
			return err
		}
		backupConfig := backupData["config"].(map[string]interface{})
		err = publishers.backupPub.Connect(backupConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

func (publishers *PrimaryBackupPublisher) Publish(subject string, msg []byte) error {
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

func (publishers *PrimaryBackupPublisher) Close() error {
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

func (publishers *PrimaryBackupPublisher) newBackupPub(protocol string) error {
	var pub publisher.JudoPub
	var err error
	switch protocol {
	case "redis":
		pub, err = redispub.New()
		if err != nil {
			return err
		}
	case "sidekiq":
		pub, err = sidekiqpub.New()
		if err != nil {
			return err
		}
	case "nano":
		pub, err = nanoreq.New()
		if err != nil {
			return err
		}
	case "nats":
		pub, err = stanpub.New()
		if err != nil {
			return err
		}
	case "pubnub":
		pub, err = pubnubPub.New()
		if err != nil {
			return err
		}
	default:
		return nil
	}
	publishers.backupPub = pub
	return nil
}
