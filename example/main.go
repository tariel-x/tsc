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
	err := Liftoff(
		os.Getenv("RMQ"),
		os.Getenv("RMQ_API"),
		"example",
		"",
		"ev_b",
		func(in DataIn) (DataOut, error) {
			fmt.Println(in.A)
			return DataOut{}, nil
		},
	)
	die(err)
}
