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

package replication

import (
	"github.com/Nextdoor/parselogical"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// WalMessage is the bifrost wal message struct we pass with stripped down fields.
type WalMessage struct {
	WalStart     uint64
	ServerWalEnd uint64
	ServerTime   int64
	TimeBasedKey string
	Pr           *parselogical.ParseResult
	PartitionKey string
}

// PgxReplicationMessageToWalMessage is a converter and validator to turn pgx ReplicationMessages to the bifrost
// format (which is stripped down).
func PgxReplicationMessageToWalMessage(pgxMsg *pgx.ReplicationMessage) (*WalMessage, error) {
	if pgxMsg == nil {
		return &WalMessage{}, errors.New("pgx.ReplicationMessage is nil")
	}

	if pgxMsg.WalMessage == nil {
		return &WalMessage{}, errors.New("pgx.ReplicationMessage.WalMessage is nil")
	}

	walString := string(pgxMsg.WalMessage.WalData)
	pr := parselogical.NewParseResult(walString)

	// Validate the ParseResult
	err := pr.ParsePrelude()
	if err != nil {
		return &WalMessage{}, err
	}

	// Parse columns
	err = pr.ParseColumns()
	if err != nil {
		return &WalMessage{}, err
	}

	walMsg := WalMessage{
		pgxMsg.WalMessage.WalStart,
		pgxMsg.WalMessage.ServerWalEnd,
		// Note that in current postgres versions (9,10,11) ServerTime is unset and comes through as 0
		int64(pgxMsg.WalMessage.ServerTime),
		"",
		pr,
		"",
	}

	return &walMsg, nil
}
