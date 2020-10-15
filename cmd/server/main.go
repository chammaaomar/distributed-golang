package main

import (
	"log"

	"github.com/chammaaomar/proglog/internal/server"
)

func main() {
	logServer := server.NewHTTPServer("localhost:8080")
	log.Fatal(logServer.ListenAndServe())
}
