package kafka

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler func(ctx context.Context, rec *kgo.Record) error

func RunConsumer(ctx context.Context, cl *kgo.Client, handle Handler) error {
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == ctx.Err() {
					return ctx.Err()
				}
				log.Printf("fetch error: %v", e)
			}
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if err := handle(ctx, rec); err != nil {
				return err
			}
		}
	}
}
