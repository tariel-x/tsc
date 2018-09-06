package main

import (
	"encoding/json"
	"errors"
	"fmt"

	tsc "github.com/tariel-x/tsc/base"

	rh "github.com/michaelklishin/rabbit-hole"
	"github.com/streadway/amqp"
	"github.com/tariel-x/polyschema"
)

type Handler func(in DataIn) (DataOut, error)

func (s Service) createInType() tsc.ExType {
	return tsc.CreateType(DataIn{})
}

func (s Service) createOutType() tsc.ExType {
	return tsc.CreateType(DataOut{})
}

type Service struct {
	Channel     *amqp.Channel
	Queue       *amqp.Queue
	Client      *rh.Client
	cfg         tsc.Config
	ListeningEx tsc.ExName
}

func New(rmq, api, name, event, emit string) (Service, error) {
	cfg, err := tsc.NewConfig(rmq, api, name, event, emit)
	if err != nil {
		return Service{}, err
	}
	s := Service{
		cfg: cfg,
	}
	return s, nil
}

func (s *Service) Liftoff(handler Handler) error {
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

func (s Service) listeningExName() tsc.ExName {
	if s.cfg.Event != "" {
		return tsc.ExName(s.cfg.Event)
	}
	return s.ListeningEx
}

func (s *Service) setListeningEx(name tsc.ExName) {
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

func (s *Service) createExchanges() error {
	var err error
	var exDataType tsc.ExType
	inDataType := s.createInType()
	if s.listeningExName() != "" {
		exDataType, err = s.getEx(s.listeningExName(), inDataType)
		if err != nil {
			return fmt.Errorf("Can not get in exchange %s: %s", s.listeningExName(), err)
		}
		equal, err := tsc.Subtype(inDataType, exDataType)
		if err != nil {
			return err
		}
		if equal == polyschema.TypesNotEqual {
			return fmt.Errorf("Can not listen event %s: incompatible types", s.listeningExName())
		}
	} else {
		var exName tsc.ExName
		exName, exDataType, err = s.searchSuitable(inDataType)
		if err != nil {
			return fmt.Errorf("Erorr while searching suitable exchange: %s", err)
		}
		s.setListeningEx(exName)
	}

	outDataType := s.createOutType()
	exDataType, err = s.getEx(tsc.ExName(s.cfg.Emit), outDataType)
	if err != nil {
		return fmt.Errorf("Can not get out exchange %s: %s", s.cfg.Emit, err)
	}
	equal, err := tsc.Subtype(exDataType, outDataType)
	if err != nil {
		return err
	}
	if equal == polyschema.TypesNotEqual {
		return fmt.Errorf("Can not emit event %s: incompatible types", s.cfg.Emit)
	}
	return nil
}

func (s Service) getEx(name tsc.ExName, dataType tsc.ExType) (tsc.ExType, error) {
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

func (s Service) searchSuitable(dataType tsc.ExType) (tsc.ExName, tsc.ExType, error) {
	excs, err := s.Client.ListExchangesIn(s.cfg.Vhost())
	if err != nil {
		return "", "", fmt.Errorf("Can not get list of exchanges: %s", err)
	}

	var foundName tsc.ExName
	var foundDataType tsc.ExType

	for _, exc := range excs {
		exDataType, err := s.exDataType(exc.Arguments)
		if err != nil {
			continue
		}
		equal, err := tsc.Subtype(exDataType, dataType)
		if err != nil {
			continue
		}
		if equal == polyschema.TypesNotEqual {
			continue
		}
		foundName = tsc.ExName(exc.Name)
		foundDataType = exDataType
		break
	}

	if foundName == "" {
		return "", "", errors.New("No one suitable exchange found")
	}

	return foundName, foundDataType, nil
}

func (s Service) createExchange(name tsc.ExName, dataType tsc.ExType) error {
	settings := rh.ExchangeSettings{
		Type:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Arguments: map[string]interface{}{
			tsc.TypeArgName: string(dataType),
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

func (s Service) exDataType(args map[string]interface{}) (tsc.ExType, error) {
	dataTypeArg, ok := args[tsc.TypeArgName]
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
	return tsc.ExType(exDataType), nil
}
