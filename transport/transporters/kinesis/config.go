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

package kinesis

import (
	"gopkg.in/Nextdoor/cli.v1"
	"gopkg.in/Nextdoor/cli.v1/altsrc"
)

const (
	ConfVarStreamName         = "kinesis-stream"
	ConfVarAwsAccessKeyId     = "aws-access-key-id"
	ConfVarAwsSecretAccessKey = "aws-secret-access-key"
	ConfVarAwsRegion          = "aws-region"
	ConfVarEndpoint           = "endpoint"
)

var Flags = []cli.Flag{
	cli.StringFlag{
		Name:  "config",
		Value: "config.yaml",
		Usage: "bifrost YAML config file",
	},
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarStreamName,
		Usage:  "kinesis stream name",
		EnvVar: "BIFROST_KINESIS_STREAM",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarAwsAccessKeyId,
		Usage:  "aws access key id",
		EnvVar: "AWS_ACCESS_KEY_ID",
		Value:  "",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarAwsSecretAccessKey,
		Usage:  "aws secret access key",
		EnvVar: "AWS_SECRET_ACCESS_KEY",
		Value:  "",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarAwsRegion,
		Usage:  "aws region",
		EnvVar: "AWS_REGION",
		Value:  "",
	}),
	altsrc.NewStringFlag(cli.StringFlag{
		Name:   ConfVarEndpoint,
		Usage:  "endpoint",
		EnvVar: "ENDPOINT",
		Value:  "",
		Hidden: true,
	}),
}
