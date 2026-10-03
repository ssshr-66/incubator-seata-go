/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xa

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
)

func TestMariaDBXAResourceFactoryRegistered(t *testing.T) {
	_, ok := GetXAResourceFactory(types.DBTypeMARIADB)

	assert.True(t, ok, "MariaDB XA resource factory should be registered")
}

func TestMariaDBXAResourceLifecycle(t *testing.T) {
	tests := []struct {
		name    string
		call    func(XAResource) error
		wantSQL string
	}{
		{
			name:    "start",
			call:    func(resource XAResource) error { return resource.Start(context.Background(), "global:1-2", TMNoFlags) },
			wantSQL: "XA START 'global:1-2'",
		},
		{
			name:    "end success",
			call:    func(resource XAResource) error { return resource.End(context.Background(), "global:1-2", TMSuccess) },
			wantSQL: "XA END 'global:1-2'",
		},
		{
			name:    "end failure",
			call:    func(resource XAResource) error { return resource.End(context.Background(), "global:1-2", TMFail) },
			wantSQL: "XA END 'global:1-2'",
		},
		{
			name:    "prepare",
			call:    func(resource XAResource) error { return resource.XAPrepare(context.Background(), "global:1-2") },
			wantSQL: "XA PREPARE 'global:1-2'",
		},
		{
			name:    "two phase commit",
			call:    func(resource XAResource) error { return resource.Commit(context.Background(), "global:1-2", false) },
			wantSQL: "XA COMMIT 'global:1-2'",
		},
		{
			name:    "one phase commit",
			call:    func(resource XAResource) error { return resource.Commit(context.Background(), "global:1-2", true) },
			wantSQL: "XA COMMIT 'global:1-2' ONE PHASE",
		},
		{
			name:    "rollback",
			call:    func(resource XAResource) error { return resource.Rollback(context.Background(), "global:1-2") },
			wantSQL: "XA ROLLBACK 'global:1-2'",
		},
		{
			name: "64 byte xid boundary",
			call: func(resource XAResource) error {
				return resource.Start(context.Background(), strings.Repeat("x", 64), TMNoFlags)
			},
			wantSQL: "XA START '" + strings.Repeat("x", 64) + "'",
		},
		{
			name: "long branch xid splits into gtrid and bqual",
			call: func(resource XAResource) error {
				return resource.Start(context.Background(), strings.Repeat("g", 64)+"-123", TMNoFlags)
			},
			wantSQL: "XA START '" + strings.Repeat("g", 64) + "','-123'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mariaDBRecordingConn{}
			resource := newMariaDBTestResource(t, conn)

			err := tt.call(resource)

			require.NoError(t, err)
			require.Len(t, conn.execQueries, 1)
			assert.Equal(t, tt.wantSQL, conn.execQueries[0])
		})
	}
}

func TestMariaDBXAResourceValidatesFlagsAndXID(t *testing.T) {
	tests := []struct {
		name string
		call func(XAResource) error
	}{
		{
			name: "start rejects join",
			call: func(resource XAResource) error { return resource.Start(context.Background(), "xid", TMJoin) },
		},
		{
			name: "end rejects suspend",
			call: func(resource XAResource) error { return resource.End(context.Background(), "xid", TMSuspend) },
		},
		{
			name: "reject empty xid",
			call: func(resource XAResource) error { return resource.Start(context.Background(), "", TMNoFlags) },
		},
		{
			name: "reject xid longer than 64 bytes",
			call: func(resource XAResource) error {
				return resource.Start(context.Background(), strings.Repeat("x", 65), TMNoFlags)
			},
		},
		{
			name: "reject backslash in xid",
			call: func(resource XAResource) error { return resource.Start(context.Background(), `xid\branch`, TMNoFlags) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mariaDBRecordingConn{}
			resource := newMariaDBTestResource(t, conn)

			err := tt.call(resource)

			assert.Error(t, err)
			assert.Empty(t, conn.execQueries)
		})
	}
}

func TestMariaDBXAResourceEscapesXIDQuotes(t *testing.T) {
	conn := &mariaDBRecordingConn{}
	resource := newMariaDBTestResource(t, conn)

	err := resource.Start(context.Background(), "global'branch", TMNoFlags)

	require.NoError(t, err)
	require.Len(t, conn.execQueries, 1)
	assert.Equal(t, "XA START 'global''branch'", conn.execQueries[0])
}

func TestMariaDBXAResourceRecover(t *testing.T) {
	conn := &mariaDBRecordingConn{
		rows: &mariaDBRecordingRows{
			columns: []string{"formatID", "gtrid_length", "bqual_length", "data"},
			values: [][]driver.Value{
				{int64(1), int64(6), int64(4), []byte("global:1-2")},
				{int64(1), int64(7), int64(0), "global3"},
			},
		},
	}
	resource := newMariaDBTestResource(t, conn)

	got, err := resource.Recover(context.Background(), TMStartRScan|TMEndRScan)

	require.NoError(t, err)
	assert.Equal(t, []string{"global:1-2", "global3"}, got)
	assert.Equal(t, "XA RECOVER", conn.query)
	assert.True(t, conn.rows.closed)
}

func TestMariaDBXAResourceRecoverRejectsInvalidRows(t *testing.T) {
	tests := []struct {
		name  string
		value []driver.Value
	}{
		{
			name:  "lengths do not match data",
			value: []driver.Value{int64(1), int64(4), int64(2), []byte("short")},
		},
		{
			name:  "negative length",
			value: []driver.Value{int64(1), int64(-1), int64(0), []byte("x")},
		},
		{
			name:  "unsupported data type",
			value: []driver.Value{int64(1), int64(1), int64(0), int64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &mariaDBRecordingConn{
				rows: &mariaDBRecordingRows{
					columns: []string{"formatID", "gtrid_length", "bqual_length", "data"},
					values:  [][]driver.Value{tt.value},
				},
			}
			resource := newMariaDBTestResource(t, conn)

			got, err := resource.Recover(context.Background(), TMStartRScan)

			assert.Error(t, err)
			assert.Nil(t, got)
			assert.True(t, conn.rows.closed)
		})
	}
}

func TestMariaDBXAResourceRecoverReturnsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	conn := &mariaDBRecordingConn{
		rows: &mariaDBRecordingRows{
			columns:  []string{"formatID", "gtrid_length", "bqual_length", "data"},
			values:   [][]driver.Value{{int64(1), int64(1), int64(0), []byte("x")}},
			closeErr: closeErr,
		},
	}
	resource := newMariaDBTestResource(t, conn)

	got, err := resource.Recover(context.Background(), TMStartRScan)

	assert.ErrorIs(t, err, closeErr)
	assert.Nil(t, got)
	assert.True(t, conn.rows.closed)
}

func TestMariaDBXAResourceRecoverReturnsQueryError(t *testing.T) {
	queryErr := errors.New("recover query failed")
	conn := &mariaDBRecordingConn{queryErr: queryErr}
	resource := newMariaDBTestResource(t, conn)

	got, err := resource.Recover(context.Background(), TMStartRScan)

	assert.ErrorIs(t, err, queryErr)
	assert.Nil(t, got)
}

func TestMariaDBXAResourcePropagatesExecError(t *testing.T) {
	execErr := errors.New("XA start failed")
	conn := &mariaDBRecordingConn{execErr: execErr}
	resource := newMariaDBTestResource(t, conn)

	err := resource.Start(context.Background(), "global:1-2", TMNoFlags)

	assert.ErrorIs(t, err, execErr)
}

func TestMariaDBXAResourceRecoverHonorsFlags(t *testing.T) {
	for _, flag := range []int{TMNoFlags, TMEndRScan} {
		t.Run(strings.TrimSpace(map[int]string{TMNoFlags: "no flags", TMEndRScan: "end scan"}[flag]), func(t *testing.T) {
			conn := &mariaDBRecordingConn{}
			resource := newMariaDBTestResource(t, conn)

			got, err := resource.Recover(context.Background(), flag)

			require.NoError(t, err)
			assert.Nil(t, got)
			assert.Empty(t, conn.query)
		})
	}

	conn := &mariaDBRecordingConn{}
	resource := newMariaDBTestResource(t, conn)
	_, err := resource.Recover(context.Background(), TMFail)
	assert.Error(t, err)
	assert.Empty(t, conn.query)
}

func TestMariaDBXAErrorClassifier(t *testing.T) {
	factory, ok := GetXAResourceFactory(types.DBTypeMARIADB)
	require.True(t, ok)
	classifier := factory.CreateErrorClassifier()

	assert.True(t, classifier.IsAlreadyEnded(&mysql.MySQLError{Number: 1399, Message: "XAER_RMFAIL: command cannot run in IDLE state"}))
	assert.True(t, classifier.IsAlreadyEnded(&mysql.MySQLError{Number: 1399, Message: "XAER_RMFAIL: command cannot run in PREPARED state"}))
	assert.False(t, classifier.IsAlreadyEnded(&mysql.MySQLError{Number: 1399, Message: "XAER_RMFAIL: unknown failure"}))
	assert.False(t, classifier.IsAlreadyEnded(&mysql.MySQLError{Number: 1400, Message: "XAER_INVAL: invalid transaction id"}))
	assert.False(t, classifier.IsAlreadyEnded(errors.New("XAER_RMFAIL: command cannot run in IDLE state")))
	assert.False(t, classifier.IsAlreadyEnded(nil))
}

func TestMariaDBXAResourceUnsupportedOperations(t *testing.T) {
	resource := newMariaDBTestResource(t, &mariaDBRecordingConn{})

	assert.Zero(t, resource.GetTransactionTimeout())
	assert.False(t, resource.SetTransactionTimeout(30))
	assert.False(t, resource.IsSameRM(context.Background(), resource))
	assert.Error(t, resource.Forget(context.Background(), "xid"))
}

func newMariaDBTestResource(t *testing.T, conn driver.Conn) XAResource {
	t.Helper()
	factory, ok := GetXAResourceFactory(types.DBTypeMARIADB)
	require.True(t, ok, "MariaDB XA resource factory should be registered")
	return factory.CreateXAResource(conn)
}

type mariaDBRecordingConn struct {
	execQueries []string
	execErr     error
	query       string
	queryErr    error
	rows        *mariaDBRecordingRows
}

func (*mariaDBRecordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (*mariaDBRecordingConn) Close() error              { return nil }
func (*mariaDBRecordingConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }

func (c *mariaDBRecordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.execQueries = append(c.execQueries, query)
	return driver.RowsAffected(0), c.execErr
}

func (c *mariaDBRecordingConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.query = query
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}

type mariaDBRecordingRows struct {
	columns  []string
	values   [][]driver.Value
	index    int
	closed   bool
	closeErr error
}

func (r *mariaDBRecordingRows) Columns() []string { return r.columns }
func (r *mariaDBRecordingRows) Close() error {
	r.closed = true
	return r.closeErr
}
func (r *mariaDBRecordingRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	if len(dest) != len(r.values[r.index]) {
		return errors.New("unexpected destination length")
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
