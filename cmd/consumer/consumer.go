package main

import (
	"context"
	"log"
)

type Consumer struct{}

func (c *Consumer) Created(ctx context.Context, uid string) error {
	log.Println("created processed:", uid)
	return nil
}

func (c *Consumer) Updated(ctx context.Context, uid string) error {
	log.Println("updated processed:", uid)
	return nil
}

func (c *Consumer) Deleted(ctx context.Context, uid string) error {
	log.Println("deleted processed:", uid)
	return nil
}
