package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Produce(ctx context.Context, cl *kgo.Client, topic string, key, value []byte) error {
	r := &kgo.Record{Topic: topic, Key: key, Value: value}
	results := cl.ProduceSync(ctx, r)
	return results.FirstErr()
}
