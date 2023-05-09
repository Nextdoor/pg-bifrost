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
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
)

var (
	logger              = logrus.New()
	log                 = logger.WithField("package", "conn")
	logProgressInterval = int64(30 * time.Second)
)

func init() {
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

// Manager here allows us to wrap our postgres connection for connection refreshes and ease of testing.
type Manager struct {
	sourceConfig    *pgconn.Config
	conn            Conn
	replicationSlot string
}

// NewManager simply returns a new Manager with the provided configuration.
func NewManager(sourceConfig *pgconn.Config, replicationSlot string) ManagerInterface {
	return &Manager{sourceConfig: sourceConfig,
		conn:            nil,
		replicationSlot: replicationSlot}
}

// GetConn idempotently returns an instance of a connection. It will make sure to re-connect if
// it is expired. This connection will NOT have replication started.
func (m *Manager) GetConn(ctx context.Context) (Conn, error) {
	return m.getConn(ctx, 0, false)
}

// GetConnWithStartLsn is the same as GetConn but starts replication at the provided LSN.
func (m *Manager) GetConnWithStartLsn(ctx context.Context, startLsn uint64) (Conn, error) {
	return m.getConn(ctx, startLsn, true)
}

func (m *Manager) getConn(ctx context.Context, startLsn uint64, startReplication bool) (Conn, error) {
	// Create a new connection
	if m.conn == nil || m.conn.IsClosed() {

		// Get a new connection
		conn, err := NewConnWithRetry(ctx, m.sourceConfig)

		if err != nil {
			return nil, err
		}

		// Start replication on the connection
		if startReplication {
			err = conn.StartReplication(context.Background(), m.replicationSlot, pglogrepl.LSN(startLsn), pglogrepl.StartReplicationOptions{PluginArgs: []string{}})
			if err != nil {
				return nil, err
			}
		}

		m.conn = conn
	}

	return m.conn, nil
}

// Close idempotently closes a connection.
func (m *Manager) Close() {
	if m.conn == nil {
		return
	}

	m.conn.Close(context.Background())
	m.conn = nil
}
