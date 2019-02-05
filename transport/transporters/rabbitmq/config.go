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

package rabbitmq

import (
	cli "gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

const (
	ConfVarExchangeName = "exchange-name"
	ConfVarUsername     = "rabbitmq-username"
	ConfVarPassword     = "rabbitmq-password"
	ConfVarHost         = "rabbitmq-host"
	ConfVarVirtualHost  = "rabbitmq-vhost"
)

var Flags = []cli.Flag{
	cli.StringFlag{
		Name:  "config",
		Value: "config.yaml",
		Usage: "bifrost YAML config file",
	},
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarExchangeName,
		Usage:  "RabbitMQ Exchange Name",
		EnvVar: "BIFROST_RABBITMQ_EXCHANGE_NAME",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarUsername,
		Usage:  "RabbitMQ Username",
		EnvVar: "BIFROST_RABBITMQ_USERNAME",
		Value:  "guest",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarPassword,
		Usage:  "RabbitMQ Password",
		EnvVar: "BIFROST_RABBITMQ_PASSWORD",
		Value:  "guest",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarHost,
		Usage:  "RabbitMQ Host",
		EnvVar: "BIFROST_RABBITMQ_HOST",
		Value:  "localhost:5672",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarVirtualHost,
		Usage:  "RabbitMQ Virtual Host",
		EnvVar: "BIFROST_RABBITMQ_VHOST",
		Value:  "/",
	}),
}
