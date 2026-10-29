package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewClient(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	base = append(base, opts...)
	return kgo.NewClient(base...)
}

func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, rf int16) error {
	admin := kadm.NewClient(cl)
	_, err := admin.CreateTopics(ctx, partitions, rf, nil, topic)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}
