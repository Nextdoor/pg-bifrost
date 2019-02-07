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

package transporter

import (
	"context"
	"testing"

	"github.com/NeowayLabs/wabbit"
	"github.com/NeowayLabs/wabbit/amqptest"
	"github.com/NeowayLabs/wabbit/amqptest/server"
)

func makeDialer(connStr string) DialerFn {
	return func() (wabbit.Conn, error) {
		return amqptest.Dial(connStr)
	}
}
func TestGetConnection(t *testing.T) {
	connString := "amqp://anyhost:anyport/%2fanyVHost"
	fakeServer := server.NewServer(connString)
	err := fakeServer.Start()
	if err != nil {
		t.Error(err)
	}
	defer fakeServer.Stop()

	connMan := NewConnectionManager(connString, log, makeDialer(connString))
	conn, err := connMan.GetConnection(context.Background())
	if err != nil {
		t.Error(err)
	}

	if conn == nil {
		t.Error()
	}
}

func TestGetConnectionAfterFailure(t *testing.T) {
	connString := "amqp://anyhost:anyport/%2fanyVHost"
	fakeServer := server.NewServer(connString)
	err := fakeServer.Start()
	if err != nil {
		t.Error(err)
	}

	connMan := NewConnectionManager(connString, log, makeDialer(connString))
	conn, err := connMan.GetConnection(context.Background())
	if err != nil {
		t.Error(err)
	}

	if conn == nil {
		t.Error()
	}
	fakeServer.Stop()

	err = fakeServer.Start()
	if err != nil {
		t.Error(err)
	}
	defer fakeServer.Stop()
	conn, err = connMan.GetConnection(context.Background())
	if err != nil {
		t.Error(err)
	}

	if conn == nil {
		t.Error()
	}
}

func TestGetConnectionAfterCancelation(t *testing.T) {
	connString := "amqp://anyhost:anyport/%2fanyVHost"
	connMan := NewConnectionManager(connString, log, func() (wabbit.Conn, error) {
		return amqptest.Dial(connString)
	})
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	conn, err := connMan.GetConnection(ctx)
	if err == nil {
		t.Error(err)
	}

	if conn != nil {
		t.Error()
	}
}
