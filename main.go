package tsc

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/mcuadros/go-jsonschema-generator"
)

type Config struct {
	Name    string // service name
	Event   string // event name
	Url     string // rmq connection string, e.g. amqp://guest:guest@localhost:5672
	AutoAck bool
}

type Service struct {
	Channel *amqp.Channel
	Queue   *amqp.Queue

	ch     chan json.RawMessage
	err    chan error
	config Config
}

type Handler func(in interface{}) (interface{}, error)

func Liftoff(cfg Config, handler Handler) error {
	conn, err := amqp.Dial(cfg.Url)
	if err != nil {
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()



	return nil
}

func detectType(in interface{}) string {
	jsonType := &jsonschema.Document{}
	jsonType.Read(in)
	return jsonType.String()
}
