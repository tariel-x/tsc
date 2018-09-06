package base

import (
	"encoding/json"
	"net/url"

	"github.com/mcuadros/go-jsonschema-generator"
	"github.com/streadway/amqp"
	"github.com/tariel-x/polyschema"
)

type (
	ExName string
	ExType string
)

const (
	TypeArgName = "datatype"
)

func Die(err error) {
	if err != nil {
		panic(err)
	}
}

func Subtype(parent, child ExType) (polyschema.TypesIdentity, error) {
	return polyschema.SubtypeRaw(string(parent), string(child))
}

func CreateType(in interface{}) ExType {
	jsonType := &jsonschema.Document{}
	jsonType.Read(in)
	marshalledType, _ := json.Marshal(jsonType)
	return ExType(marshalledType)
}

type Config struct {
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

func (cfg Config) Vhost() string {
	return cfg.uri.Vhost
}

func NewConfig(rmq, api, name, event, emit string) (Config, error) {
	cfg := Config{
		Rmq:             rmq,
		Name:            name,
		Event:           event,
		Emit:            emit,
		NumberOfWorkers: 1,
	}

	rmqURI, err := amqp.ParseURI(rmq)
	if err != nil {
		return Config{}, err
	}
	cfg.uri = rmqURI

	APIURL, err := url.Parse(api)
	if err != nil {
		return Config{}, err
	}
	cfg.APIUser = APIURL.User.Username()
	cfg.APIPassword, _ = APIURL.User.Password()
	APIURL.User = nil
	cfg.APIURL = APIURL.String()

	return cfg, nil
}
