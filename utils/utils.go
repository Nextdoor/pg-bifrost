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

package utils

import (
	"fmt"
	"github.com/Nextdoor/pg-bifrost.git/replication/client/conn"
	"github.com/cevaris/ordered_map"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/pkg/errors"
	"hash/crc32"

	"context"
)

// QuickHash buckets the string into i buckets based on crc32 hashing
func QuickHash(s string, i int) int {
	checksum := crc32.ChecksumIEEE([]byte(s))
	return int(checksum) % i
}

// PgCreateReplicationSlot is a util to create a replication slot
func PgCreateReplicationSlot(ctx context.Context, sourceConfig *pgconn.Config, slot string) error {
	rplConn, err := conn.NewConnWithRetry(ctx, sourceConfig)
	if err != nil {
		return err
	}
	defer rplConn.Close(ctx)

	_, err = rplConn.IdentifySystem(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to Identify System")
	}

	_, err = rplConn.CreateReplicationSlot(ctx, slot, "test_decoding", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		return errors.Wrapf(err, "unable to create slot %s", slot)
	}

	return err
}

// PgDropReplicationSlot is a util to drop a replication slot
func PgDropReplicationSlot(ctx context.Context, sourceConfig *pgconn.Config, slot string) error {
	rplConn, err := conn.NewConnWithRetry(ctx, sourceConfig)
	if err != nil {
		return err
	}
	defer rplConn.Close(ctx)

	_, err = rplConn.IdentifySystem(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to Identify System")
	}

	err = rplConn.DropReplicationSlot(ctx, slot, pglogrepl.DropReplicationSlotOptions{Wait: true})
	if err != nil {
		return errors.Wrapf(err, "unable to drop slot %s", slot)
	}

	return err
}

// orderedMapToString is a helper function to return ordered_map as an array of strings
func OrderedMapToStrings(om *ordered_map.OrderedMap) []string {
	builder := make([]string, om.Len())

	var index int = 0
	iter := om.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		val, _ := om.Get(kv.Key)
		builder[index] = fmt.Sprintf("%v:%v", kv.Key, val)
		index++
	}

	return builder
}
