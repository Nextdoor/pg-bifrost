/*
  Copyright 2023 Nextdoor.com, Inc.

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

type KafkaPartitionMethod int

const (
	KAFKA_PART_TXN        KafkaPartitionMethod = iota // Uses each msgs's txn to select kafka partition
	KAFKA_PART_BATCH                                  // All messages in a batch go to the same random partition
	KAFKA_PART_RANDOM                                 // Messages are randomly assigned a partition by Sarama
	KAFKA_PART_TXN_CONST                              // Similar to KAFKA_PART_TXN but should only be used for testing
	KAFKA_PART_TABLE_NAME                             // Partitions by message table name
)

var (
	NameToPartitionMethod = map[string]KafkaPartitionMethod{
		"random":               KAFKA_PART_RANDOM,
		"batch":                KAFKA_PART_BATCH,
		"transaction":          KAFKA_PART_TXN,
		"transaction-constant": KAFKA_PART_TXN_CONST,
		"tablename":            KAFKA_PART_TABLE_NAME,
	}
)
