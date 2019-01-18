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
	"github.com/jackc/pgx"
)

// PgReplConnWrapper is a wrapper struct around pgx.ReplicationConn to help with gomocks
type PgReplConnWrapper struct {
	conn *pgx.ReplicationConn
}

// New is a simple constructor which returns a pgx replication connection
func New(conf pgx.ConnConfig) (Conn, error) {
	pgcon, err := pgx.ReplicationConnect(conf)

	conn := PgReplConnWrapper{
		pgcon,
	}

	return conn, err
}

// IsAlive wraps pgx.ReplicationConn.IsAlive
func (c PgReplConnWrapper) IsAlive() bool {
	return c.IsAlive()
}

// SendStandbyStatus wraps pgx.ReplicationConn.SendStandbyStatus
func (c PgReplConnWrapper) SendStandbyStatus(status *pgx.StandbyStatus) error {
	return c.conn.SendStandbyStatus(status)
}

// WaitForReplicationMessage wraps pgx.ReplicationConn.WaitForReplicationMessage
func (c PgReplConnWrapper) WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error) {
	return c.conn.WaitForReplicationMessage(ctx)
}

// StartReplication wraps pgx.ReplicationConn.StartReplication
func (c PgReplConnWrapper) StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error) {
	return c.conn.StartReplication(slotName, startLsn, timeline, pluginArguments...)
}

// Close the connection
func (c PgReplConnWrapper) Close() error {
	return c.conn.Close()
}

// CreateReplicationSlot wraps pgx.ReplicationConn.CreateReplicationSlot
func (c PgReplConnWrapper) CreateReplicationSlot(slotName, outputPlugin string) (err error) {
	return c.conn.CreateReplicationSlot(slotName, outputPlugin)
}

// DropReplicationSlot wraps pgx.ReplicationConn.DropReplicationSlot
func (c PgReplConnWrapper) DropReplicationSlot(slotName string) (err error) {
	return c.conn.DropReplicationSlot(slotName)
}

// pgConnValid is merely used for testing whether a simple database connection can be established.
func pgConnValid(sourceConfig pgx.ConnConfig) error {
	conn, err := pgx.Connect(sourceConfig)
	defer conn.Close()

	if err != nil {
		return err
	}

	return nil
}
