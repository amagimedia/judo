package sidekiq

import (
	"encoding/json"
	"time"

	judoConfig "github.com/amagimedia/judo/v2/config"
	"github.com/amagimedia/judo/v2/publisher"
	workers "github.com/jrallison/go-workers"
)

type Config struct {
	Endpoint string
	Password string
	DB       string
	Queue    string
	Worker   string
	Job      string
}

func (c *Config) GetKeys() []string {
	return []string{"endpoint", "password", "db", "queue", "worker", "job"}
}

func (c *Config) GetMandatoryKeys() []string {
	return []string{"endpoint"}
}

func (c *Config) GetField(key string) string {
	switch key {
	case "endpoint":
		return "Endpoint"
	case "password":
		return "Password"
	case "db":
		return "DB"
	case "queue":
		return "Queue"
	case "worker":
		return "Worker"
	case "job":
		return "Job"
	default:
		return ""
	}
	return ""
}

type sidekiqPub struct {
	config *Config
}

func (pub *sidekiqPub) Connect(configs map[string]interface{}) error {

	config := &Config{}
	cfgHelper := judoConfig.ConfigHelper{config}

	err := cfgHelper.ValidateAndSet(configs)
	if err != nil {
		return err
	}

	connectOpts := map[string]string{
		"server":   config.Endpoint,
		"database": config.DB,
		"pool":     "10",
		"process":  "1",
	}

	if config.Password != "" {
		connectOpts["password"] = config.Password
	}

	workers.Configure(connectOpts)
	pub.config = config

	return err
}

func (pub *sidekiqPub) Publish(subject string, msg []byte) error {
	message := make(map[string]interface{})
	err := json.Unmarshal(msg, &message)

	_, err = workers.EnqueueWithOptions(
		pub.config.Queue,
		pub.config.Worker,
		[]interface{}{pub.config.Job, message},
		workers.EnqueueOptions{
			Retry: false,
			At:    float64((time.Now().Add(3 * time.Second)).UnixNano()) / workers.NanoSecondPrecision},
	)

	return err
}

func (pub *sidekiqPub) Close() error {
	return nil
}

func New() (publisher.JudoPub, error) {
	return &sidekiqPub{}, nil
}
