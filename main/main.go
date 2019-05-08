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

package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/Nextdoor/pg-bifrost.git/app/config"
	"github.com/Nextdoor/pg-bifrost.git/partitioner"
	"github.com/Nextdoor/pg-bifrost.git/transport/batcher"

	"github.com/Nextdoor/pg-bifrost.git/app"
	"github.com/Nextdoor/pg-bifrost.git/transport"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/kinesis"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/rabbitmq"
	"github.com/Nextdoor/pg-bifrost.git/transport/transporters/s3"
	"github.com/Nextdoor/pg-bifrost.git/utils"

	"github.com/Nextdoor/pg-bifrost.git/shutdown"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

// These vars are replaced during compile time.
var (
	Version     = "dev"
	GitRevision = "dev"
)

// Config vars
var (
	logger = logrus.New()
	log    = logger.WithField("package", "app")

	// flags that can be set via CLI, config file or environment
	flags = []cli.Flag{
		cli.StringFlag{
			Name:  config.VAR_NAME_CONFIG,
			Value: "config.yaml",
			Usage: "bifrost YAML config file",
		},
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_SLOT,
			Value:  "pg_bifrost",
			Usage:  "postgres replication slot",
			EnvVar: "REPLICATION_SLOT",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_USER,
			Value:  "replication",
			Usage:  "postgres replication user",
			EnvVar: "PGUSER",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name: config.VAR_NAME_PASSWD,
			// No default
			Usage:  "postgres replication user password",
			EnvVar: "PGPASSWORD",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_HOST,
			Value:  "127.0.0.1",
			Usage:  "postgres connection host",
			EnvVar: "PGHOST",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_PORT,
			Value:  "5432",
			Usage:  "postgres connection port",
			EnvVar: "PGPORT",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_DB,
			Value:  "postgres",
			Usage:  "postgres database name",
			EnvVar: "PGDATABASE",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_MEM_PROFILE,
			Usage:  "location to write memory pprof profile file to via SIGUSR1",
			EnvVar: "MEMPROFILE",
			Hidden: true,
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:   config.VAR_NAME_CPU_PROFILE,
			Usage:  "location to write cpu pprof profile file",
			EnvVar: "CPUPROFILE",
			Hidden: true,
		}),
	}
)

// sourceConfig is a helper to create a ConnConfig from from the cli.Context
func sourceConfig(c *cli.Context) pgx.ConnConfig {
	// postgresql://replication:nextdoor@127.0.0.1:5432/postgres
	sourceURI := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s",
		c.GlobalString(config.VAR_NAME_USER),
		c.GlobalString(config.VAR_NAME_PASSWD),
		c.GlobalString(config.VAR_NAME_HOST),
		c.GlobalString(config.VAR_NAME_PORT),
		c.GlobalString(config.VAR_NAME_DB))

	sourceConfig, err := pgx.ParseConnectionString(sourceURI)
	if err != nil {
		log.Error("unable to parse source uri", err)
		os.Exit(1)
	}

	return sourceConfig
}

// runCreate creates a replication slot
func runCreate(sourceConfig pgx.ConnConfig, replicationSlot string) error {
	err := utils.PgCreateReplicationSlot(sourceConfig, replicationSlot)

	if err != nil {
		if strings.HasSuffix(err.Error(), "(SQLSTATE 42710)") {
			fmt.Printf("Replication slot '%s' already exists.\n", replicationSlot)
			return nil
		} else {
			log.Errorf("Unable to create replication slot '%s'", replicationSlot)
			return err
		}
	}

	fmt.Printf("Created replication slot '%s'\n", replicationSlot)
	return nil
}

// runDrop drops a replication slot
func runDrop(sourceConfig pgx.ConnConfig, replicationSlot string) error {
	err := utils.PgDropReplicationSlot(sourceConfig, replicationSlot)

	if err != nil {
		log.Errorf("Unable to drop replication slot '%s'", replicationSlot)
		return err
	}

	fmt.Printf("Dropped replication slot '%s'\n", replicationSlot)
	return nil
}

func memProfile(filename string) {
	log.Warnf("writing mem profile to: %s", filename)
	f, err := os.Create(filename)
	if err != nil {
		log.Error("could not create memory profile: ", err)
	}

	// get up-to-date statistics

	runtime.GC()

	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Error("could not write memory profile: ", err)
	}

	err = f.Close()
	if err != nil {
		log.Warnf("Error writing pprof memory profile to %s %s", filename, err.Error())
	} else {
		log.Infof("Wrote pprof memory profile to: '%s'", filename)
	}
}

// runReplicate starts the replication routine
func runReplicate(
	sourceConfig pgx.ConnConfig,
	replicationSlot string,
	clientConfig map[string]interface{},
	filterConfig map[string]interface{},
	partitionerConfig map[string]interface{},
	batcherConfig map[string]interface{},
	transportType transport.TransportType,
	transportConfig map[string]interface{},
	memprofile string,
	cpuprofile string) error {

	// Start CPU profiling
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		log.Info("Starting CPU profiling")
	}

	// Configure memory profiling
	if memprofile != "" {
		runtime.MemProfileRate = 100
	}

	// Create a shared cancellation context for application shutdown .
	// We use this to pass both the cancellation context and canceller function to our submodules.
	shutdownHandler := shutdown.NewShutdownHandler()

	runner, err := app.New(shutdownHandler,
		sourceConfig,
		replicationSlot,
		clientConfig,
		filterConfig,
		partitionerConfig,
		batcherConfig,
		transportType,
		transportConfig)

	if err != nil {
		log.Error("failed to start runner")
		return err
	}

	// Start all the modules
	go runner.Start()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)

	// Signal Handling
	go func() {
		stoppedCpuProfiling := false

		for {
			sig := <-sigchan
			log.Infof("Got a signal %d (%s)", sig, sig.String())

			// If it's a SIGUSR1 and the cpuprofile flag was set, then dump pprof files and continue running.
			// https://golang.org/pkg/runtime/pprof
			if sig == syscall.SIGUSR1 && cpuprofile != "" {
				if stoppedCpuProfiling != true {
					log.Warn("Stopping CPU profiling")
					pprof.StopCPUProfile()
					log.Infof("Wrote pprof cpu profile to: '%s'", cpuprofile)

					// bifrost only supports gathering of CPU profiling data once. This flag will permanantly disable
					// profiling in the existing runtime and further SIGUSR1 signals will not finalize it again.
					stoppedCpuProfiling = true
				}

				continue
			}

			// If it's a SIGUSR2 and the memprofile flag was set, then dump pprof file and continue running.
			// NOTE: there can be multiple invocations of memprofile to dump new, updated pprof files.
			// https://golang.org/pkg/runtime/pprof
			if sig == syscall.SIGUSR2 && memprofile != "" {
				time.Sleep(1 * time.Second)
				memProfile(fmt.Sprintf(memprofile))

				continue
			}

			log.Info("pg-bifrost received a shutdown")

			// Stop profiling on shutdown
			if cpuprofile != "" && stoppedCpuProfiling != true {
				log.Warn("Stopping CPU profiling")
				pprof.StopCPUProfile()
			}

			if memprofile != "" {
				time.Sleep(1 * time.Second)
				memProfile(fmt.Sprintf("%s.stop", memprofile))
			}

			// initiates all modules to run their shutdown() function
			shutdownHandler.CancelFunc()
		}
	}()

	// block on a shutdownHandler cancellation
	<-shutdownHandler.TerminateCtx.Done()

	// Wait a bit for routines to stop
	timeout := time.NewTimer(10 * time.Second)
	<-timeout.C

	return nil
}

// getFlagValue returns the actual primitive value of a Flag Generic Value
func getFlagValue(flagGeneric interface{}) interface{} {
	if flagGeneric == nil {
		return nil
	}

	v := reflect.ValueOf(flagGeneric).MethodByName("Get").Call([]reflect.Value{})

	if len(v) != 1 {
		log.Fatal("wrong number of expected return values")
	}

	return v[0].Interface()
}

func replicateAction(c *cli.Context) error {
	if c.GlobalBool(config.VAR_NAME_CREATE_SLOT) {
		err := runCreate(sourceConfig(c), c.GlobalString(config.VAR_NAME_SLOT))
		if err != nil {
			return err
		}
	}

	//
	// Validate and create client config from Flags
	//
	clientBufferSize := c.GlobalInt(config.VAR_NAME_CLIENT_BUFFER_SIZE)
	if clientBufferSize < 1 {
		return errors.New("'" + config.VAR_NAME_CLIENT_BUFFER_SIZE + "'" + " must be a positive integer")
	}

	clientConfig := make(map[string]interface{}, 0)
	clientConfig[config.VAR_NAME_CLIENT_BUFFER_SIZE] = clientBufferSize

	//
	// Validate and create transport config from Flags
	//
	workerNum := c.GlobalInt(config.VAR_NAME_WORKERS)
	if workerNum < 1 {
		return errors.New("'" + config.VAR_NAME_WORKERS + "'" + " must be a positive integer")
	}

	transportConfig := make(map[string]interface{}, 0)
	for _, flagName := range c.FlagNames() {
		transportConfig[flagName] = getFlagValue(c.Generic(flagName))
	}
	transportConfig[config.VAR_NAME_WORKERS] = workerNum

	// pass partition and routing config to transport so it can make decisions based on these.
	transportConfig[config.VAR_NAME_PARTITION_METHOD] = partitioner.GetPartitionMethod(c.GlobalString(config.VAR_NAME_PARTITION_METHOD))
	transportConfig[config.VAR_NAME_BATCHER_ROUTING_METHOD] = batcher.GetRoutingMethod(c.GlobalString(config.VAR_NAME_BATCHER_ROUTING_METHOD))

	//
	// Validate, construct and create filter config from Flags
	//
	wl := c.GlobalStringSlice(config.VAR_NAME_WHITELIST)
	bl := c.GlobalStringSlice(config.VAR_NAME_BLACKLIST)
	var tablelist []string
	var whitelist bool
	if len(wl) != 0 && len(bl) != 0 {
		return errors.New("'whitelist' and 'blacklist' are mutually exclusive. Use one or the other but not both")
	} else if len(wl) != 0 {
		whitelist = true
		tablelist = wl
	} else {
		whitelist = false
		tablelist = bl
	}

	filterConfig := make(map[string]interface{}, 0)
	filterConfig["whitelist"] = whitelist
	filterConfig["tablelist"] = tablelist

	//
	// Validate, construct and create partitioner config from Flags
	//
	partMethod := partitioner.GetPartitionMethod(c.GlobalString(config.VAR_NAME_PARTITION_METHOD))

	partitionCount := c.GlobalInt(config.VAR_NAME_PARTITION_COUNT)
	if partitionCount < 1 {
		return errors.New("'" + config.VAR_NAME_PARTITION_COUNT + "'" + " must be a positive integer")
	}

	partitionerConfig := make(map[string]interface{}, 0)
	partitionerConfig[config.VAR_NAME_PARTITION_METHOD] = partMethod
	partitionerConfig[config.VAR_NAME_PARTITION_COUNT] = partitionCount

	//
	// Validate and create batcher config from Flags
	//
	batchFlushMaxAge := c.GlobalInt(config.VAR_NAME_BATCH_FLUSH_MAX_AGE)
	if batchFlushMaxAge < 1 {
		return errors.New("'" + config.VAR_NAME_BATCH_FLUSH_MAX_AGE + "' must be a positive integer")
	}

	batchFlushUpdateAge := c.GlobalInt(config.VAR_NAME_BATCH_FLUSH_UPDATE_AGE)
	if batchFlushUpdateAge < 1 {
		return errors.New("'" + config.VAR_NAME_BATCH_FLUSH_UPDATE_AGE + "' must be a positive integer")
	}

	batchQueueDepth := c.GlobalInt(config.VAR_NAME_BATCH_QUEUE_DEPTH)
	if batchQueueDepth < 1 {
		return errors.New("'" + config.VAR_NAME_BATCH_QUEUE_DEPTH + "'" + " must be a positive integer")
	}

	batcherMemorySoftLimit := c.GlobalInt64(config.VAR_NAME_BATCHER_MEMORY_SOFT_LIMIT)
	if batcherMemorySoftLimit < 1 {
		return errors.New("'" + config.VAR_NAME_BATCHER_MEMORY_SOFT_LIMIT + "'" + " must be a positive integer")
	}

	batcherRoutingMethod := batcher.GetRoutingMethod(c.GlobalString(config.VAR_NAME_BATCHER_ROUTING_METHOD))

	batcherConfig := make(map[string]interface{}, 0)
	batcherConfig[config.VAR_NAME_BATCH_FLUSH_MAX_AGE] = batchFlushMaxAge
	batcherConfig[config.VAR_NAME_BATCH_FLUSH_UPDATE_AGE] = batchFlushUpdateAge
	batcherConfig[config.VAR_NAME_BATCH_QUEUE_DEPTH] = batchQueueDepth
	batcherConfig[config.VAR_NAME_BATCHER_MEMORY_SOFT_LIMIT] = batcherMemorySoftLimit
	batcherConfig[config.VAR_NAME_BATCHER_ROUTING_METHOD] = batcherRoutingMethod

	//
	// Start replication
	//
	log.Info("Replicating to ", c.Command.Name)

	return runReplicate(
		sourceConfig(c),
		c.GlobalString(config.VAR_NAME_SLOT),
		clientConfig,
		filterConfig,
		partitionerConfig,
		batcherConfig,
		transport.TransportType(c.Command.Name),
		transportConfig,
		c.GlobalString(config.VAR_NAME_MEM_PROFILE),
		c.GlobalString(config.VAR_NAME_CPU_PROFILE),
	)
}

// main parses configuration information and runs a command based on that
func main() {
	cli_app := cli.NewApp()
	cli_app.Version = fmt.Sprintf("%s-%s", Version, GitRevision)

	// The precedence for flag value sources is as follows (highest to lowest):
	//
	// 0. Command line flag value from user
	// 1. Environment variable (if specified)
	// 2. Configuration file (if specified)
	// 3. Default defined on the flag
	cli_app.Before = altsrc.InitInputSourceWithContext(flags, altsrc.NewYamlSourceFromFlagFunc("config"))
	cli_app.Flags = flags

	// Also set up kinesis subcommands flags from file
	kinesisCmd := cli.Command{
		Name:   "kinesis",
		Usage:  "replicate to kinesis",
		Action: replicateAction,
	}

	kinesisCmd.Before = altsrc.InitInputSourceWithContext(kinesis.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))
	kinesisCmd.Flags = kinesis.Flags

	// Also set up s3 subcommands flags from file
	s3Cmd := cli.Command{
		Name:   "s3",
		Usage:  "replicate to s3",
		Action: replicateAction,
	}

	s3Cmd.Before = altsrc.InitInputSourceWithContext(s3.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))
	s3Cmd.Flags = s3.Flags

	// Also set up rabbitmq subcommands flags from file
	rabbitmqCmd := cli.Command{
		Name:   "rabbitmq",
		Usage:  "replicate to rabbitmq",
		Action: replicateAction,
	}

	rabbitmqCmd.Before = altsrc.InitInputSourceWithContext(rabbitmq.Flags, altsrc.NewYamlSourceFromFlagFunc("config"))
	rabbitmqCmd.Flags = rabbitmq.Flags

	cli_app.Commands = []cli.Command{
		{
			Name:    "create",
			Aliases: []string{"c"},
			Usage:   "create a replication slot",
			Action: func(c *cli.Context) error {
				return runCreate(sourceConfig(c), c.GlobalString(config.VAR_NAME_SLOT))
			},
		},
		{
			Name:    "drop",
			Aliases: []string{"d"},
			Usage:   "drop a replication slot",
			Action: func(c *cli.Context) error {
				return runDrop(sourceConfig(c), c.GlobalString(config.VAR_NAME_SLOT))
			},
		},
		{
			Name:    "replicate",
			Aliases: []string{"r"},
			Usage:   "start logical replication",
			Action: func(c *cli.Context) error {
				cli.ShowCommandHelpAndExit(c, "replicate", 1)
				return nil
			},
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:   fmt.Sprintf("%s, %s", config.VAR_NAME_CREATE_SLOT, config.VAR_NAME_SHORT_CREATE_SLOT),
					Usage:  "Creates replication slot if it doesn't exist before replicating",
					EnvVar: "CREATE_SLOT",
				},
				altsrc.NewIntFlag(cli.IntFlag{
					Name:   config.VAR_NAME_WORKERS,
					Value:  1,
					Usage:  "number of workers for transport",
					EnvVar: "WORKERS",
				}),
				altsrc.NewIntFlag(cli.IntFlag{
					Name:   config.VAR_NAME_CLIENT_BUFFER_SIZE,
					Value:  10000,
					Usage:  "number of messages to buffer from postgres",
					EnvVar: "CLIENT_BUFFER_SIZE",
				}),
				altsrc.NewIntFlag(cli.IntFlag{
					Name:  config.VAR_NAME_BATCH_FLUSH_UPDATE_AGE,
					Value: 500,
					Usage: "amount of time to wait in milliseconds for a new message before batch is written. If a " +
						"new message is added then the timer is reset. This is evaluated every second.",
					EnvVar: "BATCH_FLUSH_UPDATE_AGE",
				}),
				altsrc.NewIntFlag(cli.IntFlag{
					Name:  config.VAR_NAME_BATCH_FLUSH_MAX_AGE,
					Value: 1000,
					Usage: "maximum amount of time in milliseconds to wait before writing a batch. This overrides " +
						"'" + config.VAR_NAME_BATCH_FLUSH_UPDATE_AGE + "'. This is evaluated every second.",
					EnvVar: "BATCH_FLUSH_MAX_AGE",
				}),
				altsrc.NewIntFlag(cli.IntFlag{
					Name:   config.VAR_NAME_BATCH_QUEUE_DEPTH,
					Value:  2,
					Usage:  "number of batches that can be queued per worker",
					EnvVar: "BATCH_QUEUE_DEPTH",
				}),
				altsrc.NewInt64Flag(cli.Int64Flag{
					Name:  config.VAR_NAME_BATCHER_MEMORY_SOFT_LIMIT,
					Value: batcher.DEFAULT_MAX_MEMORY_BYTES,
					Usage: "maximum amount of memory to use when batching messages. Note this is only evaluated " +
						"every 'batch-flush-timeout'. Also, note that if you use the 'partition' " +
						"batcher-routing-method and have a high cardinality partition key you may " +
						"need to tweak this value to create batches of meaningful size.",
					EnvVar: "BATCHER_MEMORY_SOFT_LIMIT",
				}),
				altsrc.NewStringFlag(cli.StringFlag{
					Name:  config.VAR_NAME_BATCHER_ROUTING_METHOD,
					Value: "round-robin",
					Usage: "determines how to route batches to workers. Options are 'round-robin' (default) " +
						"and 'partition'. If you require strict ordering of data then use 'partition'.",
					EnvVar: "BATCHER_ROUTING_METHOD",
				}),
				altsrc.NewIntFlag(cli.IntFlag{
					Name:  config.VAR_NAME_PARTITION_COUNT,
					Value: 1,
					Usage: "number of buckets to use when bucketing partitions in partition-method=" +
						"'transaction-bucket'.",
					EnvVar: "PARTITION_COUNT",
				}),
				altsrc.NewStringFlag(cli.StringFlag{
					Name:  config.VAR_NAME_PARTITION_METHOD,
					Value: "none",
					Usage: "determines how messages will be split into batches. Options are 'none' (default) " +
						", 'tablename', 'transaction', and 'transaction-bucket'. 'transaction' will ensure that " +
						"a batch will only ever have messages from a single transaction. This can be dangerous if " +
						"your database does lots of small transactions. 'transaction-bucket' performs a hash to " +
						"pick the partition. This will mean that entire transactions will go into the same " +
						"partition but that partition may have other transactions as well. 'tablename' partitions " +
						"on the table name of the message.",
					EnvVar: "PARTITION_METHOD",
				}),
				cli.StringSliceFlag{
					Name:   config.VAR_NAME_WHITELIST,
					Usage:  "A whitelist of tables to include. All others will be excluded.",
					EnvVar: "WHITELIST",
				},
				cli.StringSliceFlag{
					Name:   config.VAR_NAME_BLACKLIST,
					Usage:  "A blacklist of tables to exclude. All others will be included.",
					EnvVar: "BLACKLIST",
				},
			},
			Subcommands: []cli.Command{
				{
					Name:   "stdout",
					Usage:  "replicate to stdout",
					Action: replicateAction,
				},
				kinesisCmd,
				s3Cmd,
				rabbitmqCmd,
			},
		},
	}

	err := cli_app.Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}
