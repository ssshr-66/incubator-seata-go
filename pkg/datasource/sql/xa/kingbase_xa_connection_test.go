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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/mock"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
)

type kingbaseMockRows struct {
	rows     [][]driver.Value
	index    int
	closed   bool
	closeErr error
}

func (r *kingbaseMockRows) Columns() []string { return []string{"gid"} }

func (r *kingbaseMockRows) Close() error {
	r.closed = true
	return r.closeErr
}

func (r *kingbaseMockRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func TestKingbaseXAConnStartAndEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	tx := mock.NewMockTestDriverTx(ctrl)
	conn.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Return(tx, nil)

	xaConn := &KingbaseXAConn{Conn: conn}
	require.NoError(t, xaConn.Start(context.Background(), "xid", TMNoFlags))
	assert.NoError(t, xaConn.End(context.Background(), "xid", TMSuccess))
	assert.NoError(t, xaConn.End(context.Background(), "xid", TMFail))
	assert.Error(t, xaConn.End(context.Background(), "xid", TMJoin))
	assert.Error(t, xaConn.Start(context.Background(), "xid", TMJoin))
}

func TestKingbaseXAConnPrepareQuotesAndBoundsGID(t *testing.T) {
	t.Run("quote literal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		conn := mock.NewMockTestDriverConn(ctrl)
		tx := mock.NewMockTestDriverTx(ctrl)
		conn.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Return(tx, nil)
		conn.EXPECT().ExecContext(gomock.Any(), "PREPARE TRANSACTION 'seata-go:kingbase:x'' ; DROP TABLE t; --'", gomock.Any()).
			Return(&driver.ResultNoRows, nil)

		xaConn := &KingbaseXAConn{Conn: conn}
		require.NoError(t, xaConn.Start(context.Background(), "x' ; DROP TABLE t; --", TMNoFlags))
		require.NoError(t, xaConn.XAPrepare(context.Background(), "x' ; DROP TABLE t; --"))
		assert.Nil(t, xaConn.tx)
	})

	t.Run("reject GID beyond server byte limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		conn := mock.NewMockTestDriverConn(ctrl)
		xaConn := &KingbaseXAConn{Conn: conn}
		err := xaConn.Start(context.Background(), strings.Repeat("x", 182), TMNoFlags)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "199 bytes")
	})
}

func TestKingbaseGIDRoundTripAndByteBoundary(t *testing.T) {
	xid := strings.Repeat("x", 181)
	gid, err := kingbaseGID(xid)
	require.NoError(t, err)
	assert.Len(t, []byte(gid), 199)

	got, owned, err := decodeKingbaseGID(gid)
	require.NoError(t, err)
	assert.True(t, owned)
	assert.Equal(t, xid, got)

	_, err = kingbaseGID(strings.Repeat("x", 182))
	assert.Error(t, err)
	_, err = kingbaseGID("bad\\xid")
	assert.Error(t, err)
	_, err = kingbaseGID("bad\x00xid")
	assert.Error(t, err)
	_, err = kingbaseGID(string([]byte{0xff}))
	assert.Error(t, err)
}

func TestKingbaseXAConnCommitAndRollbackPrepared(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	conn.EXPECT().ExecContext(gomock.Any(), "COMMIT PREPARED 'seata-go:kingbase:xid'", gomock.Any()).
		Return(&driver.ResultNoRows, nil)
	conn.EXPECT().ExecContext(gomock.Any(), "ROLLBACK PREPARED 'seata-go:kingbase:xid'", gomock.Any()).
		Return(&driver.ResultNoRows, nil)

	xaConn := &KingbaseXAConn{Conn: conn}
	assert.NoError(t, xaConn.Commit(context.Background(), "xid", false))
	assert.NoError(t, xaConn.Rollback(context.Background(), "xid"))
}

func TestKingbaseXAConnOnePhaseCommitAndActiveRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	tx := mock.NewMockTestDriverTx(ctrl)
	conn.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Return(tx, nil).Times(2)
	tx.EXPECT().Commit().Return(nil)
	tx.EXPECT().Rollback().Return(nil)

	xaConn := &KingbaseXAConn{Conn: conn}
	require.NoError(t, xaConn.Start(context.Background(), "commit-xid", TMNoFlags))
	require.NoError(t, xaConn.Commit(context.Background(), "commit-xid", true))
	assert.Nil(t, xaConn.tx)
	require.NoError(t, xaConn.Start(context.Background(), "rollback-xid", TMNoFlags))
	require.NoError(t, xaConn.Rollback(context.Background(), "rollback-xid"))
	assert.Nil(t, xaConn.tx)
}

func TestKingbaseXAConnRecoverFiltersAndDecodesGIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	rows := &kingbaseMockRows{rows: [][]driver.Value{
		{[]byte("seata-go:kingbase:xid-one")},
		{"unrelated-prepared-transaction"},
		{"seata-go:kingbase:xid-two"},
	}}
	conn.EXPECT().QueryContext(gomock.Any(), "SELECT gid FROM sys_prepared_xacts WHERE database = current_database()", gomock.Any()).
		Return(rows, nil)

	xaConn := &KingbaseXAConn{Conn: conn}
	xids, err := xaConn.Recover(context.Background(), TMStartRScan|TMEndRScan)
	require.NoError(t, err)
	assert.Equal(t, []string{"xid-one", "xid-two"}, xids)
	assert.True(t, rows.closed)
}

func TestKingbaseXAConnRecoverErrorsAndFlags(t *testing.T) {
	t.Run("reject unsupported flags", func(t *testing.T) {
		xaConn := &KingbaseXAConn{}
		_, err := xaConn.Recover(context.Background(), TMJoin)
		assert.Error(t, err)
	})

	t.Run("reject malformed adapter GID", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		conn := mock.NewMockTestDriverConn(ctrl)
		rows := &kingbaseMockRows{rows: [][]driver.Value{{"seata-go:kingbase:"}}}
		conn.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, nil)

		xaConn := &KingbaseXAConn{Conn: conn}
		_, err := xaConn.Recover(context.Background(), TMStartRScan)
		require.Error(t, err)
		assert.True(t, rows.closed)
	})

	t.Run("reject missing rows handle", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		conn := mock.NewMockTestDriverConn(ctrl)
		conn.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

		xaConn := &KingbaseXAConn{Conn: conn}
		_, err := xaConn.Recover(context.Background(), TMStartRScan)
		assert.ErrorContains(t, err, "no rows handle")
	})

	t.Run("reject unsupported GID value type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		conn := mock.NewMockTestDriverConn(ctrl)
		rows := &kingbaseMockRows{rows: [][]driver.Value{{int64(7)}}}
		conn.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, nil)

		xaConn := &KingbaseXAConn{Conn: conn}
		_, err := xaConn.Recover(context.Background(), TMStartRScan)
		assert.ErrorContains(t, err, "unsupported GID type")
		assert.True(t, rows.closed)
	})

	t.Run("close rows and return close error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		conn := mock.NewMockTestDriverConn(ctrl)
		rows := &kingbaseMockRows{closeErr: errors.New("close rows")}
		conn.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(rows, nil)

		xaConn := &KingbaseXAConn{Conn: conn}
		_, err := xaConn.Recover(context.Background(), TMStartRScan)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "close rows")
		assert.True(t, rows.closed)
	})
}

func TestKingbaseXAResourceFactoryRegistered(t *testing.T) {
	factory, ok := GetXAResourceFactory(types.DBTypeKingbase)
	require.True(t, ok)
	assert.IsType(t, &KingbaseXAErrorClassifier{}, factory.CreateErrorClassifier())
	assert.False(t, factory.CreateErrorClassifier().IsAlreadyEnded(errors.New("unknown server error")))
}
