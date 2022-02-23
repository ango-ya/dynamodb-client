package dynamodb

import (
	"context"
	"reflect"
)

type Handler func(ctx context.Context, in interface{}) (out interface{}, err error)

type Middleware func(next Handler) Handler

func applyMiddlewares(f Handler, middle []Middleware) Handler {
	last := f
	for i := len(middle) - 1; i >= 0; i-- {
		last = middle[i](last)
	}
	return last
}

func (d *DynamoDB) printLog(next Handler) Handler {
	return func(ctx context.Context, in interface{}) (out interface{}, err error) {
		d.logger.Info().Msgf("%s", getTypeName(in))
		return next(ctx, in)
	}
}

func (d *DynamoDB) setTimeout(next Handler) Handler {
	return func(ctx context.Context, in interface{}) (out interface{}, err error) {
		ctx, cancel := context.WithTimeout(ctx, d.timeout)
		defer cancel()

		return next(ctx, in)
	}
}

func getTypeName(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}
