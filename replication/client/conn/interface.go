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

// Conn is a interface which both our PgReplConnWrapper and pgx.ReplicationConn implement to help with gomocks
// To generate a new mock:

//go:generate mockgen -destination=mocks/mock_client.go -package=mocks github.com/Nextdoor/pg-bifrost.git/replication/client/conn Conn
type Conn interface {
	IsAlive() bool
	SendStandbyStatus(status *pgx.StandbyStatus) error
	WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error)
	StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error)
	CreateReplicationSlot(slotName, outputPlugin string) (err error)
	DropReplicationSlot(slotName string) (err error)
	Close() error
}

//go:generate mockgen -destination=mocks/mock_manager.go -package=mocks github.com/Nextdoor/pg-bifrost.git/replication/client/conn ManagerInterface
type ManagerInterface interface {
	GetConn() (Conn, error)
	Close()
}
