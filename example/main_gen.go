package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/mcuadros/go-jsonschema-generator"
	rh "github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
	"github.com/tariel-x/polyschema"
)

const (
	typeArgName = "datatype"
)

type Handler func(in DataIn) (DataOut, error)

func (s Service) createInType() ExType {
	return s.createType(DataIn{})
}

func (s Service) createOutType() ExType {
	return s.createType(DataOut{})
}

type (
	ExName string
	ExType string
)

type config struct {
	Name            string // service name
	Event           string // event name
	Emit            string // event name
	AutoAck         bool
	NumberOfWorkers int

	Rmq         string
	uri         amqp.URI
	APIURL      string
	APIUser     string
	APIPassword string
}

func (cfg config) Vhost() string {
	return cfg.uri.Vhost
}

func newConfig(rmq, api, name, event, emit string) (config, error) {
	cfg := config{
		Rmq:             rmq,
		Name:            name,
		Event:           event,
		Emit:            emit,
		NumberOfWorkers: 1,
	}

	rmqURI, err := amqp.ParseURI(rmq)
	if err != nil {
		return config{}, err
	}
	cfg.uri = rmqURI

	APIURL, err := url.Parse(api)
	if err != nil {
		return config{}, err
	}
	cfg.APIUser = APIURL.User.Username()
	cfg.APIPassword, _ = APIURL.User.Password()
	APIURL.User = nil
	cfg.APIURL = APIURL.String()

	return cfg, nil
}

type Service struct {
	Channel     *amqp.Channel
	Queue       *amqp.Queue
	Client      *rh.Client
	cfg         config
	ListeningEx ExName
}

func Liftoff(rmq, api, name, event, emit string, handler Handler) error {
	cfg, err := newConfig(rmq, api, name, event, emit)
	if err != nil {
		return err
	}
	s := Service{
		cfg: cfg,
	}

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
	if err := s.createListenQueue(); err != nil {
		return fmt.Errorf("Can not create listening queue: %s", err)
	}

	return s.listen(handler)
}

func (s Service) listeningExName() ExName {
	if s.cfg.Event != "" {
		return ExName(s.cfg.Event)
	}
	return s.ListeningEx
}

func (s *Service) setListeningEx(name ExName) {
	s.ListeningEx = name
}

func (s *Service) connectAPI() error {
	client, err := rh.NewClient(s.cfg.APIURL, s.cfg.APIUser, s.cfg.APIPassword)
	if err != nil {
		return fmt.Errorf("Can not connect RMQ API: %s", err)
	}
	s.Client = client
	return nil
}

func (s *Service) createListenQueue() error {
	q, err := s.Channel.QueueDeclare(
		s.cfg.Name,
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Can not declare queue %s", err)
	}
	s.Queue = &q

	return s.Channel.QueueBind(
		q.Name,
		"",
		string(s.listeningExName()),
		false,
		nil,
	)
}

func (s Service) createExchanges() error {
	var err error
	var exDataType ExType
	inDataType := s.createInType()
	if s.listeningExName() != "" {
		exDataType, err = s.getEx(s.listeningExName(), inDataType)
		if err != nil {
			return fmt.Errorf("Can not get in exchange %s: %s", s.listeningExName(), err)
		}
		equal, err := s.subtype(inDataType, exDataType)
		if err != nil {
			return err
		}
		if equal == polyschema.TypesNotEqual {
			return fmt.Errorf("Can not listen event %s: incompatible types", s.listeningExName())
		}
	} else {
		var exName ExName
		exName, exDataType, err = s.searchSuitable(inDataType)
		if err != nil {
			return fmt.Errorf("Erorr while searching suitable exchange: %s", err)
		}
		s.setListeningEx(exName)
	}

	outDataType := s.createOutType()
	exDataType, err = s.getEx(ExName(s.cfg.Emit), outDataType)
	if err != nil {
		return fmt.Errorf("Can not get out exchange %s: %s", s.cfg.Emit, err)
	}
	equal, err := s.subtype(exDataType, outDataType)
	if err != nil {
		return err
	}
	if equal == polyschema.TypesNotEqual {
		return fmt.Errorf("Can not emit event %s: incompatible types", s.cfg.Emit)
	}
	return nil
}

func (s Service) getEx(name ExName, dataType ExType) (ExType, error) {
	exc, err := s.Client.GetExchange(s.cfg.Vhost(), string(name))
	if err != nil && err.Error() != "Error 404 (Object Not Found): Not Found" {
		return "", fmt.Errorf("Can not get exchange %s: %s", name, err)
	}
	if exc == nil {
		return dataType, s.createExchange(name, dataType)
	}

	exDataType, err := s.exDataType(exc.Arguments)
	if err != nil {
		return "", fmt.Errorf("Exchange %s exists but can not be user: %s", err, name)
	}

	return exDataType, nil
}

func (s Service) searchSuitable(dataType ExType) (ExName, ExType, error) {
	excs, err := s.Client.ListExchangesIn(s.cfg.Vhost())
	if err != nil {
		return "", "", fmt.Errorf("Can not get list of exchanges: %s", err)
	}

	var foundName ExName
	var foundDataType ExType

	for _, exc := range excs {
		exDataType, err := s.exDataType(exc.Arguments)
		if err != nil {
			continue
		}
		equal, err := s.subtype(exDataType, dataType)
		if err != nil {
			continue
		}
		if equal == polyschema.TypesNotEqual {
			continue
		}
		foundName = ExName(exc.Name)
		foundDataType = exDataType
		break
	}

	if foundName == "" {
		return "", "", errors.New("No one suitable exchange found")
	}

	return foundName, foundDataType, nil
}

func (s Service) createExchange(name ExName, dataType ExType) error {
	settings := rh.ExchangeSettings{
		Type:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Arguments: map[string]interface{}{
			typeArgName: string(dataType),
		},
	}
	_, err := s.Client.DeclareExchange(s.cfg.Vhost(), string(name), settings)
	return err
}

func (s Service) listen(handler Handler) error {
	msgs, err := s.Channel.Consume(
		s.Queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for msg := range msgs {
		copy := msg
		err := s.processInput(&copy, handler)
		if err != nil {
			fmt.Println("Error handling:", err)
		}
		copy.Ack(false)
	}

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

func (s Service) exDataType(args map[string]interface{}) (ExType, error) {
	dataTypeArg, ok := args[typeArgName]
	if !ok {
		return "", errors.New("no datatype argument")
	}
	exDataType, ok := dataTypeArg.(string)
	if !ok {
		return "", errors.New("datatype argument is not a string")
	}
	if exDataType == "" {
		return "", errors.New("datatype is empty")
	}
	return ExType(exDataType), nil
}

func (s Service) createType(in interface{}) ExType {
	jsonType := &jsonschema.Document{}
	jsonType.Read(in)
	marshalledType, _ := json.Marshal(jsonType)
	return ExType(marshalledType)
}

func (s Service) subtype(parent, child ExType) (polyschema.TypesIdentity, error) {
	return polyschema.SubtypeRaw(string(parent), string(child))
}

func die(err error) {
	if err != nil {
		panic(err)
	}
}
