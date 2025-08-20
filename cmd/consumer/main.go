package main

import (
	"context"
	"flag"
	"log"
)

var (
	amqpUrl      string
	amqpExchange string
	declareQueue bool
)

func init() {
	flag.StringVar(&amqpUrl, "amqp-url", "amqp://guest:guest@localhost:5672", "URL do RabbitMQ")
	flag.StringVar(&amqpExchange, "amqp-exchange", "user-events", "Exchange do RabbitMQ")
	flag.BoolVar(&declareQueue, "amqp-declare-queue", false, "Declare fila no RabbitMQ")
	flag.Parse()
}

func main() {
	if declareQueue {
		if err := Declare(); err != nil {
			log.Printf("can`t declare queue or exchange, err: %s", err.Error())
		}
	}

	log.Print("starting consume")

	if err := Consume(context.Background()); err != nil {
		log.Printf("can't consume messages, err: %s", err)
	}
}
