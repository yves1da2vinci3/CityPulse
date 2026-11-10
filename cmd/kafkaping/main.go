package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	brokers := config.Brokers()
	cl, err := cpkafka.NewClient(brokers)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)
	meta, err := admin.BrokerMetadata(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stdout, "brokers=%v nodes=%d\n", brokers, len(meta.Brokers))
	for _, b := range meta.Brokers {
		fmt.Fprintf(os.Stdout, "- id=%d %s:%d\n", b.NodeID, b.Host, b.Port)
	}
}
