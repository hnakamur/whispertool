package main

import (
	"flag"
	"log"

	"github.com/hnakamur/whispertool"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	baseDir := flag.String("base", ".", "base directory")
	flag.Parse()

	if err := whispertool.RunWebApp(*addr, *baseDir); err != nil {
		log.Fatal(err)
	}
}
