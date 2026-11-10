package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func main() {
	user := env("KAFKA_USER", "alice")
	pass := env("KAFKA_PASS", "alice-secret")
	broker := env("KAFKA_BROKERS", "localhost:9095")
	mech := plain.Auth{User: user, Pass: pass}.AsMechanism()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.SASL(mech))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	topics, err := kadm.NewClient(cl).ListTopics(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("topics", len(topics))
	for name := range topics {
		fmt.Println("-", name)
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
