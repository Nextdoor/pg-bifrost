/*
  Copyright 2019 Nextdoor.com, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package transporter

import (
	"context"
	"sync"
	"time"

	"github.com/NeowayLabs/wabbit"
	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
)

//go:generate mockgen -destination=mock_transporter.go -package=mocks github.com/Nextdoor/pg-bifrost.git/transport/transporters/rabbitmq/transporter ConnectionGetter

// ConnectionGetter can get a wabbit.Conn or an error
type ConnectionGetter interface {
	GetConnection(ctx context.Context) (wabbit.Conn, error)
}

// DialerFn type for functions that return a (wabbit.Conn, error) tuple
type DialerFn func() (wabbit.Conn, error)

// ConnMan holds an wabbit.Conn, and a mutex for blocking while it tries
// to get a connection
type ConnMan struct {
	lock        sync.Mutex
	dialer      DialerFn
	conn        wabbit.Conn
	retryPolicy backoff.BackOff
	log         *logrus.Entry
}

// NewConnectionManager returns a ConnMan, with a built-in backoff retryPolicy
func NewConnectionManager(amqpURL string, log *logrus.Entry, dialer DialerFn) *ConnMan {
	retryPolicy := &backoff.ExponentialBackOff{
		InitialInterval:     1500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.2,
		MaxInterval:         5 * time.Minute,
		Clock:               backoff.SystemClock,
	}

	return &ConnMan{
		dialer:      dialer,
		retryPolicy: retryPolicy,
		log:         log,
	}
}

// GetConnection blocks until it returns a RabbitMQ connection, or the retry
// gave up, or the context was cancelled.
func (cm *ConnMan) GetConnection(ctx context.Context) (wabbit.Conn, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if cm.conn == nil {
		operation := func() error {
			cm.log.Info("Trying to connect to RabbitMQ")
			conn, err := cm.dialer()
			if err != nil {
				return err
			}
			cm.conn = conn
			return nil
		}

		err := backoff.Retry(operation, backoff.WithContext(cm.retryPolicy, ctx))
		if err != nil {
			cm.log.WithError(err).Error("Could not connect to RabbitMQ")
			return nil, err
		}

		closeNotify := cm.conn.NotifyClose(make(chan wabbit.Error))
		go cm.CloseHandler(ctx, closeNotify)
	}

	return cm.conn, nil
}

// CloseHandler listens for connection close signals and sets connection
// to nil so that it can be recreated
func (cm *ConnMan) CloseHandler(ctx context.Context, closeNotify chan wabbit.Error) {
	select {
	case <-ctx.Done():
		cm.log.Debug("Connection Manager received context cancellation")
	case err := <-closeNotify:
		cm.log.Warn(err.Error())
		cm.lock.Lock()
		defer cm.lock.Unlock()
		cm.conn = nil
	}
}
