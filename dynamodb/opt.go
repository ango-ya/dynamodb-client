package dynamodb

import (
	"time"

	"github.com/rs/zerolog"
)

var (
	DefaultTimeout = 10 * time.Second
)

type Option interface {
	Apply(*DynamoDB) error
}

type Timeout time.Duration

func (o Timeout) Apply(d *DynamoDB) error {
	d.timeout = time.Duration(o)
	return nil
}
func WithTimeout(timeout time.Duration) Timeout {
	return Timeout(timeout)
}

type Logger zerolog.Logger

func (o Logger) Apply(d *DynamoDB) error {
	d.logger = zerolog.Logger(o)
	return nil
}
func WithLogger(logger zerolog.Logger) Logger {
	return Logger(logger)
}
