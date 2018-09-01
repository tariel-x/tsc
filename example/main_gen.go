package main

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/mcuadros/go-jsonschema-generator"
	rh "github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
	"github.com/tariel-x/polyschema"
)

type Config struct {
	Name            string // service name
	Event           string // event name
	Emit            string // event name
	AutoAck         bool
	NumberOfWorkers int

	Rmq         string
	uri         amqp.URI
	ApiURL      string
	ApiUser     string
	ApiPassword string
}

func NewConfig(rmq, api, name, event, emit string) (Config, error) {
	cfg := Config{
		Name:            name,
		Event:           event,
		Emit:            emit,
		NumberOfWorkers: 1,
		AutoAck:         true,
	}

	rmqUri, err := amqp.ParseURI(rmq)
	if err != nil {
		return Config{}, err
	}
	cfg.uri = rmqUri

	apiUrl, err := url.Parse(api)
	if err != nil {
		return Config{}, err
	}
	cfg.ApiUser = apiUrl.User.Username()
	cfg.ApiPassword, _ = apiUrl.User.Password()
	apiUrl.User = nil
	cfg.ApiURL = apiUrl.String()

	return cfg, nil
}

func (cfg Config) Vhost() string {
	return cfg.uri.Vhost
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

type Handler func(in DataIn) (DataOut, error)

func (s *Service) Liftoff(handler Handler) error {
	if err := s.connect(); err != nil {
		return err
	}

	return s.createExchanges()
}

func (s *Service) connect() error {
	conn, err := amqp.Dial(s.config.Rmq)
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

	client, err := rh.NewClient(s.config.ApiURL, s.config.ApiUser, s.config.ApiPassword)
	if err != nil {
		return err
	}
	s.Client = client
	return nil
}

func (s Service) createExchanges() error {
	inDataType := s.createInType()
	exDataType, err := s.lookForExchange(s.config.Event, inDataType)
	if err != nil {
		return err
	}
	equal, err := polyschema.SubtypeRaw(inDataType, exDataType)
	if err != nil {
		return err
	}
	if equal == polyschema.TypesNotEqual {
		return fmt.Errorf("Can not listen event %s: incompatible types", s.config.Event)
	}

	outDataType := s.createOutType()
	exDataType, err = s.lookForExchange(s.config.Emit, outDataType)
	if err != nil {
		return err
	}
	equal, err = polyschema.SubtypeRaw(exDataType, outDataType)
	if err != nil {
		return err
	}
	if equal == polyschema.TypesNotEqual {
		return fmt.Errorf("Can not emit event %s: incompatible types", s.config.Emit)
	}
	return nil
}

func (s Service) lookForExchange(name, dataType string) (string, error) {
	exc, err := s.Client.GetExchange(s.config.Vhost(), s.config.Event)
	if err != nil {
		return "", err
	}
	if exc == nil {
		return dataType, s.createExchange(name, dataType)
	}

	dataTypeArg, ok := exc.Arguments["datatype"]
	if !ok {
		return "", fmt.Errorf("Exchange %s exists but is untyped", name)
	}
	exDataType, ok := dataTypeArg.(string)
	if !ok {
		return "", fmt.Errorf("Exchange %s exists but type argument is not string", name)
	}
	return exDataType, nil
}

func (s Service) createExchange(name, datatype string) error {
	settings := rh.ExchangeSettings{
		Type:       "fanout",
		Durable:    false,
		AutoDelete: false,
		Arguments: map[string]interface{}{
			"datatype": datatype,
		},
	}
	_, err := s.Client.DeclareExchange(s.config.Vhost(), name, settings)
	return err
}

func (s Service) createType(in interface{}) string {
	jsonType := &jsonschema.Document{}
	jsonType.Read(in)
	return jsonType.String()
}

//
// Generated
//
func (s Service) createInType() string {
	return s.createType(DataIn{})
}

func (s Service) createOutType() string {
	return s.createType(DataOut{})
}
