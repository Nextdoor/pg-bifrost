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

package conn

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"time"
)

// PgReplConnWrapper is a wrapper struct around pgx.ReplicationConn to help with gomocks
type PgReplConnWrapper struct {
	conn *pgconn.PgConn
}

// New is a simple constructor which returns a pgx replication connection
func New(ctx context.Context, conf *pgconn.Config) (Conn, error) {
	pgcon, err := pgconn.ConnectConfig(ctx, conf)

	conn := PgReplConnWrapper{
		pgcon,
	}

	return conn, err
}

// getConnWithRetry wraps New with a retry loop. It returns a
// new replication connection without starting replication.
func NewConnWithRetry(ctx context.Context, sourceConfig *pgconn.Config) (Conn, error) {
	var conn Conn
	var err error

	retryPolicy := &backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         backoff.DefaultMaxInterval,
		MaxElapsedTime:      time.Second * 20,
		Clock:               backoff.SystemClock,
	}

	operation := func() error {
		log.Infof("Attempting to create a connection to %s on %d", sourceConfig.Host,
			sourceConfig.Port)
		conn, err = New(ctx, sourceConfig)

		return err
	}

	err = backoff.Retry(operation, retryPolicy)
	if err != nil {
		// Handle error.
		return nil, err
	}

	return conn, err
}

// IsClosed wraps pgconn.PgConn.IsClosed
func (c PgReplConnWrapper) IsClosed() bool {
	return c.conn.IsClosed()
}

// SendStandbyStatus wraps pglogrepl.SendStandbyStatusUpdate
func (c PgReplConnWrapper) SendStandbyStatus(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.conn, status)
}

// WaitForReplicationMessage wraps pgconn.PgConn.ReceiveMessage
func (c PgReplConnWrapper) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return c.conn.ReceiveMessage(ctx)
}

// StartReplication wraps pglogrepl.StartReplication
func (c PgReplConnWrapper) StartReplication(ctx context.Context, slotName string, startLSN pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error {
	return pglogrepl.StartReplication(ctx, c.conn, slotName, startLSN, options)
}

// Close the connection
func (c PgReplConnWrapper) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

// CreateReplicationSlot wraps pglogrepl.CreateReplicationSlot
func (c PgReplConnWrapper) CreateReplicationSlot(
	ctx context.Context,
	slotName string,
	outputPlugin string,
	options pglogrepl.CreateReplicationSlotOptions) (pglogrepl.CreateReplicationSlotResult, error) {
	return pglogrepl.CreateReplicationSlot(ctx, c.conn, slotName, outputPlugin, options)
}

// IdentifySystem wraps pglogrepl.IdentifySystem
func (c PgReplConnWrapper) IdentifySystem(ctx context.Context) (pglogrepl.IdentifySystemResult, error) {
	return pglogrepl.IdentifySystem(ctx, c.conn)
}

// DropReplicationSlot wraps pglogrepl.DropReplicationSlot
func (c PgReplConnWrapper) DropReplicationSlot(ctx context.Context, slotName string, options pglogrepl.DropReplicationSlotOptions) error {
	return pglogrepl.DropReplicationSlot(ctx, c.conn, slotName, options)
}
