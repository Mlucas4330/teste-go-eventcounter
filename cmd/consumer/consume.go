package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

var (
	conn    *amqp091.Connection
	channel *amqp091.Channel
)

func getChannel() (*amqp091.Channel, error) {
	var err error
	if channel != nil && !channel.IsClosed() {
		return channel, nil
	}

	conn, err = amqp091.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	ch := make(chan *amqp091.Error, 2)
	channel.NotifyClose(ch)
	go func() {
		for {
			msg := <-ch
			log.Printf("channel closed, err: %s", msg)
		}
	}()

	if channel.IsClosed() {
		return nil, errors.New("channel closed")
	}

	return channel, nil
}

func Declare() error {
	channel, err := getChannel()
	if err != nil {
		return err
	}

	if err := channel.ExchangeDeclare(amqpExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}

	if _, err := channel.QueueDeclare("eventcountertest", true, false, false, false, nil); err != nil {
		return err
	}

	if err := channel.QueueBind("eventcountertest", "*.event.*", amqpExchange, false, nil); err != nil {
		return err
	}

	return nil
}

func startWorker(ch <-chan eventcounter.Message, eventType eventcounter.EventType, cw *eventcounter.ConsumerWrapper, counts map[eventcounter.EventType]map[string]int, lock *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()

	for msg := range ch {
		switch eventType {
		case eventcounter.EventCreated:
			cw.Created(context.Background(), msg.UID)
		case eventcounter.EventUpdated:
			cw.Updated(context.Background(), msg.UID)
		case eventcounter.EventDeleted:
			cw.Deleted(context.Background(), msg.UID)
		}

		lock.Lock()
		if _, ok := counts[eventType]; !ok {
			counts[eventType] = make(map[string]int)
		}
		counts[eventType][msg.UserID]++
		lock.Unlock()
	}
}

func Consume(ctx context.Context) error {
	channel, err := getChannel()
	if err != nil {
		return err
	}

	if channel.IsClosed() {
		return errors.New("channel is closed")
	}

	msgs, err := channel.Consume("eventcountertest", "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	consumer := Consumer{}
	cw := eventcounter.ConsumerWrapper{Consumer: &consumer}

	processed := make(map[string]bool)
	createdCh := make(chan eventcounter.Message, 100)
	updatedCh := make(chan eventcounter.Message, 100)
	deletedCh := make(chan eventcounter.Message, 100)
	counts := make(map[eventcounter.EventType]map[string]int)
	var countsLock sync.Mutex
	var wg sync.WaitGroup
	workerCount := 5

	for i := 0; i < workerCount; i++ {
		wg.Add(3)
		go startWorker(createdCh, eventcounter.EventCreated, &cw, counts, &countsLock, &wg)
		go startWorker(updatedCh, eventcounter.EventUpdated, &cw, counts, &countsLock, &wg)
		go startWorker(deletedCh, eventcounter.EventDeleted, &cw, counts, &countsLock, &wg)
	}

	timer := time.NewTimer(5 * time.Second)

	for {
		select {
		case d := <-msgs:
			var mb eventcounter.MessageBody
			if err := json.Unmarshal(d.Body, &mb); err != nil {
				log.Printf("error in unmarshal: %v", err)
				continue
			}

			parts := strings.Split(d.RoutingKey, ".")
			message := eventcounter.Message{
				UID:       mb.UID,
				UserID:    parts[0],
				EventType: eventcounter.EventType(parts[2]),
			}

			if !processed[message.UID] {
				processed[message.UID] = true
				switch message.EventType {
				case eventcounter.EventCreated:
					createdCh <- message
				case eventcounter.EventUpdated:
					updatedCh <- message
				case eventcounter.EventDeleted:
					deletedCh <- message
				}
			}

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(5 * time.Second)

		case <-timer.C:
			close(createdCh)
			close(updatedCh)
			close(deletedCh)

			wg.Wait()

			log.Print("stopped after 5s of no new messages")

			for eventType, users := range counts {
				if err := createAndWriteFile("data", string(eventType), users); err != nil {
					log.Printf("error writing file for %s: %s", eventType, err)
				}
			}

			return nil
		}
	}
}
