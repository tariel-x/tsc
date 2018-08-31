package tsc

import (
	"encoding/json"

	"github.com/mcuadros/go-jsonschema-generator"
	rh "github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
)

type Config struct {
	Name     string // service name
	Event    string // event name
	URL      string // rmq connection string, e.g. amqp://guest:guest@localhost:5672
	Vhost    string
	APIURL   string
	User     string
	Password string
	AutoAck  bool
}

type Service struct {
	Channel *amqp.Channel
	Queue   *amqp.Queue
	Client  *rh.Client

	ch     chan json.RawMessage
	err    chan error
	config Config
}

func New(cfg Config) Service {
	return Service{
		config: cfg,
	}
}

type Handler func(in interface{}) (interface{}, error)

func (s Service) lookForExchange() error {
	exc, err := s.Client.GetExchange(s.config.Vhost, s.config.Event)
	if err != nil {
		return err
	}
	if exc == nil {
		return s.createExchange()
	}

	// check type of an exchange

	return nil
}

func (s Service) createExchange() error {
	settings := rh.ExchangeSettings{
		Type:       "fanout",
		Durable:    false,
		AutoDelete: false,
	}

	// Set type to exchange arguments

	_, err := s.Client.DeclareExchange(s.config.Vhost, s.config.Event, settings)

	return err
}

func (s Service) Liftoff(handler Handler) error {
	conn, err := amqp.Dial(s.config.URL)
	if err != nil {
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	s.Channel = ch

	client, err := rh.NewClient(s.config.APIURL, s.config.User, s.config.Password)
	if err != nil {
		return err
	}
	s.Client = client

	return s.lookForExchange()
}

func detectType(in interface{}) string {
	jsonType := &jsonschema.Document{}
	jsonType.Read(in)
	return jsonType.String()
}
