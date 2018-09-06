package main

import (
	"fmt"
	"os"

	tsc "github.com/tariel-x/tsc/base"
)

// go:generate tsc DataIn DataOut

type DataIn struct {
	A string `json:"a"`
}

type DataOut struct {
	B int `json:"b"`
}

func main() {
	s, err := New(
		os.Getenv("RMQ"),
		os.Getenv("RMQ_API"),
		"example",
		"",
		"ev_b",
	)
	tsc.Die(err)

	err = s.Liftoff(
		func(in DataIn) (DataOut, error) {
			fmt.Println(in.A)
			return DataOut{}, nil
		},
	)
	tsc.Die(err)
}
