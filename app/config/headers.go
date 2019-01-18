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

package config

// Global cli flag names
const (
	VAR_NAME_CONFIG      = "config"
	VAR_NAME_SLOT        = "slot"
	VAR_NAME_USER        = "user"
	VAR_NAME_PASSWD      = "password"
	VAR_NAME_HOST        = "host"
	VAR_NAME_PORT        = "port"
	VAR_NAME_DB          = "dbname"
	VAR_NAME_MEM_PROFILE = "memprofile"
	VAR_NAME_CPU_PROFILE = "cpuprofile"
)

// Replicate cli flag names
const (
	VAR_NAME_CREATE_SLOT               = "create-slot"
	VAR_NAME_SHORT_CREATE_SLOT         = "s"
	VAR_NAME_WORKERS                   = "workers"
	VAR_NAME_CLIENT_BUFFER_SIZE        = "client-buffer-size"
	VAR_NAME_BATCH_FLUSH_UPDATE_AGE    = "batch-flush-update-age"
	VAR_NAME_BATCH_FLUSH_MAX_AGE       = "batch-flush-max-age"
	VAR_NAME_BATCH_QUEUE_DEPTH         = "batch-queue-depth"
	VAR_NAME_BATCHER_MEMORY_SOFT_LIMIT = "batcher-memory-soft-limit"
	VAR_NAME_BATCHER_ROUTING_METHOD    = "batcher-routing-method"
	VAR_NAME_WHITELIST                 = "whitelist"
	VAR_NAME_BLACKLIST                 = "blacklist"
	VAR_NAME_PARTITION_METHOD          = "partition-method"
	VAR_NAME_PARTITION_COUNT           = "partition-count"
)
