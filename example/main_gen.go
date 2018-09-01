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

type config struct {
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

func newConfig(rmq, api, name, event, emit string) (config, error) {
	cfg := config{
		Rmq:             rmq,
		Name:            name,
		Event:           event,
		Emit:            emit,
		NumberOfWorkers: 1,
		AutoAck:         true,
	}

	rmqUri, err := amqp.ParseURI(rmq)
	if err != nil {
		return config{}, err
	}
	cfg.uri = rmqUri

	apiUrl, err := url.Parse(api)
	if err != nil {
		return config{}, err
	}
	cfg.ApiUser = apiUrl.User.Username()
	cfg.ApiPassword, _ = apiUrl.User.Password()
	apiUrl.User = nil
	cfg.ApiURL = apiUrl.String()

	return cfg, nil
}

func (cfg config) Vhost() string {
	return cfg.uri.Vhost
}

type Service struct {
	Channel *amqp.Channel
	Queue   *amqp.Queue
	Client  *rh.Client

	cfg config
}

func newService(cfg config) Service {
	return Service{
		cfg: cfg,
	}
}

type Handler func(in DataIn) (DataOut, error)

func Liftoff(rmq, api, name, event, emit string, handler Handler) error {
	cfg, err := newConfig(rmq, api, name, event, emit)
	if err != nil {
		return err
	}
	s := newService(cfg)

	// connect RMQ
	conn, err := amqp.Dial(s.cfg.Rmq)
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

	// connect RMQ API
	if err := s.connectAPI(); err != nil {
		return err
	}

	// declare or check exchanges
	if err := s.createExchanges(); err != nil {
		return err
	}

	// declare input queue
	if err := s.createQueue(); err != nil {
		return err
	}

	return s.listen(handler)
}

func (s Service) listen(handler Handler) error {
	msgs, err := s.Channel.Consume(
		s.Queue.Name, // queue
		"",           // consumer
		true,         // auto ack
		false,        // exclusive
		false,        // no local
		false,        // no wait
		nil,          // args
	)
	if err != nil {
		return err
	}

	forever := make(chan bool)
	fmt.Println("Liftoff")
	go func() {
		for msg := range msgs {
			copy := msg
			err := s.processInput(&copy, handler)
			if err != nil {
				fmt.Println("Error handling:", err)
			}
			copy.Ack(false)
		}
	}()

	<-forever
	return nil
}

func (s Service) processInput(msg *amqp.Delivery, handler Handler) error {
	input := DataIn{}
	err := json.Unmarshal(msg.Body, &input)
	if err != nil {
		return fmt.Errorf("Can not unmarhsall input data: %s", err)
	}
	output, err := handler(input)
	if err != nil {
		return fmt.Errorf("Can not handle event: %s", err)
	}
	outBody, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("Can not marshall output data: %s", err)
	}

	return s.publish(outBody)
}

func (s Service) publish(outBody []byte) error {
	return s.Channel.Publish(
		s.cfg.Emit,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        outBody,
		})
}

func (s *Service) connectAPI() error {
	client, err := rh.NewClient(s.cfg.ApiURL, s.cfg.ApiUser, s.cfg.ApiPassword)
	if err != nil {
		return fmt.Errorf("Can not connect RMQ API: %s", err)
	}
	s.Client = client
	return nil
}

func (s *Service) createQueue() error {
	q, err := s.Channel.QueueDeclare(
		s.cfg.Name,
		true,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	s.Queue = &q

	return s.Channel.QueueBind(
		q.Name,
		"",
		s.cfg.Event,
		false,
		nil,
	)
}

func (s Service) createExchanges() error {
	inDataType := s.createInType()
	exDataType, err := s.lookForExchange(s.cfg.Event, inDataType)
	if err != nil {
		return err
	}
	equal, err := polyschema.SubtypeRaw(inDataType, exDataType)
	if err != nil {
		return err
	}
	if equal == polyschema.TypesNotEqual {
		return fmt.Errorf("Can not listen event %s: incompatible types", s.cfg.Event)
	}

	outDataType := s.createOutType()
	exDataType, err = s.lookForExchange(s.cfg.Emit, outDataType)
	if err != nil {
		return err
	}
	equal, err = polyschema.SubtypeRaw(exDataType, outDataType)
	if err != nil {
		return err
	}
	if equal == polyschema.TypesNotEqual {
		return fmt.Errorf("Can not emit event %s: incompatible types", s.cfg.Emit)
	}
	return nil
}

func (s Service) lookForExchange(name, dataType string) (string, error) {
	exc, err := s.Client.GetExchange(s.cfg.Vhost(), name)
	if err != nil && err.Error() != "Error 404 (Object Not Found): Not Found" {
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
		Durable:    true,
		AutoDelete: false,
		Arguments: map[string]interface{}{
			"datatype": datatype,
		},
	}
	_, err := s.Client.DeclareExchange(s.cfg.Vhost(), name, settings)
	return err
}

func (s Service) createType(in interface{}) string {
	jsonType := &jsonschema.Document{}
	jsonType.Read(in)
	marshalledType, _ := json.Marshal(jsonType)
	return string(marshalledType)
}

func die(err error) {
	if err != nil {
		panic(err)
	}
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
