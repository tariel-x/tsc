package main

import (
	"fmt"
	"os"
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
	die(err)

	err = s.Liftoff(
		func(in DataIn) (DataOut, error) {
			fmt.Println(in.A)
			return DataOut{}, nil
		},
	)
	die(err)
}
