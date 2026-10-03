/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/sijms/go-ora/v2/network"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/mock"
)

type oracleMockRows struct {
	rows  [][]driver.Value
	index int
}

func (r *oracleMockRows) Columns() []string { return []string{"FORMATID", "GLOBALID", "BRANCHID"} }

func (r *oracleMockRows) Close() error { return nil }

func (r *oracleMockRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func TestOracleXAConnLifecycleCalls(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		call      func(*OracleXAConn) error
		wantFlag  int64
		wantPhase int64
		wantQuery string
	}{
		{
			name:      "start",
			operation: "start",
			call:      func(c *OracleXAConn) error { return c.Start(context.Background(), "global-7", TMNoFlags) },
			wantFlag:  TMNoFlags,
			wantQuery: "DBMS_XA.XA_START",
		},
		{
			name:      "end rollback flag maps to success",
			operation: "end",
			call:      func(c *OracleXAConn) error { return c.End(context.Background(), "global-7", TMFail) },
			wantFlag:  TMSuccess,
			wantQuery: "DBMS_XA.XA_END",
		},
		{
			name:      "commit two phase",
			operation: "commit",
			call:      func(c *OracleXAConn) error { return c.Commit(context.Background(), "global-7", false) },
			wantPhase: 0,
			wantQuery: "DBMS_XA.XA_COMMIT",
		},
		{
			name:      "commit one phase",
			operation: "commit",
			call:      func(c *OracleXAConn) error { return c.Commit(context.Background(), "global-7", true) },
			wantPhase: 1,
			wantQuery: "DBMS_XA.XA_COMMIT",
		},
		{
			name:      "rollback",
			operation: "rollback",
			call:      func(c *OracleXAConn) error { return c.Rollback(context.Background(), "global-7") },
			wantQuery: "DBMS_XA.XA_ROLLBACK",
		},
		{
			name:      "forget",
			operation: "forget",
			call:      func(c *OracleXAConn) error { return c.Forget(context.Background(), "global-7") },
			wantQuery: "DBMS_XA.XA_FORGET",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			conn := mock.NewMockTestDriverConn(ctrl)
			conn.EXPECT().ExecContext(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
					require.Contains(t, query, test.wantQuery)
					values := namedValues(args)
					require.Equal(t, int64(oracleXAFormatID), values["format_id"])
					require.Equal(t, "676c6f62616c", values["gtrid"])
					require.Equal(t, "2d37", values["bqual"])
					if test.operation == "start" || test.operation == "end" {
						require.Equal(t, test.wantFlag, values["flag"])
					}
					if test.operation == "commit" {
						require.Equal(t, test.wantPhase, values["one_phase"])
					}
					setOracleOutput(t, values["xa_rc"], oracleXAOK)
					setOracleOutput(t, values["oracle_error"], 0)
					return &driver.ResultNoRows, nil
				})

			require.NoError(t, test.call(&OracleXAConn{Conn: conn}))
		})
	}
}

func TestOracleXAConnPrepareReadOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	conn.EXPECT().ExecContext(gomock.Any(), oracleXAPrepareSQL, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
			values := namedValues(args)
			setOracleOutput(t, values["xa_rc"], oracleXAReadOnly)
			setOracleOutput(t, values["oracle_error"], 0)
			return &driver.ResultNoRows, nil
		})

	err := (&OracleXAConn{Conn: conn}).XAPrepare(context.Background(), "global-7")
	require.ErrorIs(t, err, ErrXAReadOnly)
}

func TestOracleXAConnRejectsInvalidXIDBeforeDriverCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	err := (&OracleXAConn{Conn: conn}).Start(context.Background(), "invalid-xid", TMNoFlags)
	require.Error(t, err)
}

func TestOracleXAConnNonzeroReturnCode(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	conn.EXPECT().ExecContext(gomock.Any(), oracleXAPrepareSQL, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
			values := namedValues(args)
			setOracleOutput(t, values["xa_rc"], oracleXAERNOTA)
			setOracleOutput(t, values["oracle_error"], 24756)
			return &driver.ResultNoRows, nil
		})

	err := (&OracleXAConn{Conn: conn}).XAPrepare(context.Background(), "global-7")
	var xaErr *OracleXAError
	require.ErrorAs(t, err, &xaErr)
	require.Equal(t, int64(oracleXAERNOTA), xaErr.Code)
	require.Equal(t, int64(24756), xaErr.OracleCode)
}

func TestOracleXAConnPhaseTwoIsIdempotentForMissingBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	conn.EXPECT().ExecContext(gomock.Any(), oracleXACommitSQL, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
			values := namedValues(args)
			setOracleOutput(t, values["xa_rc"], oracleXAERNOTA)
			setOracleOutput(t, values["oracle_error"], 0)
			return &driver.ResultNoRows, nil
		})
	require.NoError(t, (&OracleXAConn{Conn: conn}).Commit(context.Background(), "global-7", false))
}

func TestOracleXAConnPhaseTwoTreatsORA24756AsMissingBranch(t *testing.T) {
	tests := []struct {
		name  string
		query string
		call  func(*OracleXAConn) error
	}{
		{
			name:  "commit",
			query: oracleXACommitSQL,
			call:  func(c *OracleXAConn) error { return c.Commit(context.Background(), "global-7", false) },
		},
		{
			name:  "rollback",
			query: oracleXARollbackSQL,
			call:  func(c *OracleXAConn) error { return c.Rollback(context.Background(), "global-7") },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := mock.NewMockTestDriverConn(gomock.NewController(t))
			conn.EXPECT().ExecContext(gomock.Any(), test.query, gomock.Any()).Return(nil, &network.OracleError{ErrCode: 24756})
			require.NoError(t, test.call(&OracleXAConn{Conn: conn}))
		})
	}
}

func TestOracleXAConnPhaseTwoDoesNotSwallowOtherOracleErrors(t *testing.T) {
	conn := mock.NewMockTestDriverConn(gomock.NewController(t))
	conn.EXPECT().ExecContext(gomock.Any(), oracleXACommitSQL, gomock.Any()).Return(nil, &network.OracleError{ErrCode: 1031})
	err := (&OracleXAConn{Conn: conn}).Commit(context.Background(), "global-7", false)
	require.Error(t, err)
}

func TestOracleXAErrorClassifier(t *testing.T) {
	classifier := &OracleXAErrorClassifier{}
	require.True(t, classifier.IsAlreadyEnded(&OracleXAError{Operation: "end", Code: oracleXAERNOTA}))
	require.False(t, classifier.IsAlreadyEnded(&OracleXAError{Operation: "prepare", Code: oracleXAERNOTA}))
	require.False(t, classifier.IsAlreadyEnded(errors.New("ORA-01031: insufficient privileges")))
	require.False(t, classifier.IsAlreadyEnded(&OracleXAError{Operation: "end", Code: -7, OracleCode: 12514}))
}

func TestOracleXAConnRecover(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	conn.EXPECT().QueryContext(gomock.Any(), oracleXARecoverSQL, gomock.Any()).DoAndReturn(
		func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			require.Contains(t, query, "FROM DBA_PENDING_TRANSACTIONS")
			require.Equal(t, int64(oracleXAFormatID), args[0].Value)
			return &oracleMockRows{rows: [][]driver.Value{
				{int64(123), []byte("foreign"), []byte("-8")},
				{int64(oracleXAFormatID), []byte("global-xid"), []byte("-42")},
			}}, nil
		})

	xids, err := (&OracleXAConn{Conn: conn}).Recover(context.Background(), TMStartRScan|TMEndRScan)
	require.NoError(t, err)
	require.Equal(t, []string{"global-xid-42"}, xids)
}

func TestOracleXAConnRecoverInvalidQualifier(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockTestDriverConn(ctrl)
	conn.EXPECT().QueryContext(gomock.Any(), oracleXARecoverSQL, gomock.Any()).Return(
		&oracleMockRows{rows: [][]driver.Value{{int64(oracleXAFormatID), []byte("global"), []byte("bad")}}}, nil)

	_, err := (&OracleXAConn{Conn: conn}).Recover(context.Background(), TMStartRScan)
	require.Error(t, err)
}

func TestOracleXAConnRecoverDoesNotQueryWithoutStartScan(t *testing.T) {
	conn := mock.NewMockTestDriverConn(gomock.NewController(t))
	xids, err := (&OracleXAConn{Conn: conn}).Recover(context.Background(), TMEndRScan)
	require.NoError(t, err)
	require.Empty(t, xids)
}

func namedValues(args []driver.NamedValue) map[string]any {
	values := make(map[string]any, len(args))
	for _, arg := range args {
		values[arg.Name] = arg.Value
	}
	return values
}

func setOracleOutput(t *testing.T, value any, result int64) {
	t.Helper()
	out, ok := value.(sql.Out)
	require.True(t, ok)
	dest, ok := out.Dest.(*int64)
	require.True(t, ok)
	*dest = result
}

func TestOracleXAStatementsUseBoundXID(t *testing.T) {
	for _, statement := range []string{oracleXAStartSQL, oracleXAEndSQL, oracleXAPrepareSQL, oracleXACommitSQL, oracleXARollbackSQL, oracleXAForgetSQL} {
		require.NotContains(t, strings.ToUpper(statement), "GLOBAL-XID")
		require.Contains(t, statement, "HEXTORAW(:gtrid)")
		require.Contains(t, statement, "HEXTORAW(:bqual)")
	}
}

func TestParseOracleXID(t *testing.T) {
	got, err := parseOracleXID("global-xid-42")
	require.NoError(t, err)
	require.Equal(t, int64(oracleXAFormatID), got.FormatID)
	require.Equal(t, []byte("global-xid"), got.GlobalID)
	require.Equal(t, []byte("-42"), got.BranchID)
	require.Equal(t, "676c6f62616c2d786964", got.GlobalIDHex)
	require.Equal(t, "2d3432", got.BranchIDHex)

	decoded, err := decodeOracleXID(got.FormatID, got.GlobalID, got.BranchID)
	require.NoError(t, err)
	require.Equal(t, "global-xid-42", decoded)
}

func TestOracleXIDFieldLengthBoundaries(t *testing.T) {
	globalID := strings.Repeat("g", oracleXIDFieldMaxBytes)
	xid := globalID + "-18446744073709551615"
	parsed, err := parseOracleXID(xid)
	require.NoError(t, err)
	require.Len(t, parsed.GlobalID, oracleXIDFieldMaxBytes)
	require.Len(t, parsed.BranchID, len("-18446744073709551615"))

	decoded, err := decodeOracleXID(oracleXAFormatID, parsed.GlobalID, parsed.BranchID)
	require.NoError(t, err)
	require.Equal(t, xid, decoded)

	_, err = parseOracleXID(strings.Repeat("g", oracleXIDFieldMaxBytes+1) + "-1")
	require.Error(t, err)
	_, err = decodeOracleXID(oracleXAFormatID, []byte("g"), bytes.Repeat([]byte("1"), oracleXIDFieldMaxBytes+1))
	require.Error(t, err)
}

func TestParseOracleXIDRejectsInvalidInput(t *testing.T) {
	for _, xid := range []string{
		"",
		"global",
		"global-",
		"global-0",
		"global-01",
		"global-18446744073709551616",
		"-1",
	} {
		t.Run(xid, func(t *testing.T) {
			_, err := parseOracleXID(xid)
			require.Error(t, err)
		})
	}
}

func TestDecodeOracleXIDRejectsInvalidRows(t *testing.T) {
	for _, test := range []struct {
		name     string
		formatID int64
		globalID []byte
		branchID []byte
	}{
		{name: "foreign format ID", formatID: oracleXAFormatID + 1, globalID: []byte("global"), branchID: []byte("-1")},
		{name: "empty global ID", formatID: oracleXAFormatID, globalID: nil, branchID: []byte("-1")},
		{name: "empty branch ID", formatID: oracleXAFormatID, globalID: []byte("global"), branchID: nil},
		{name: "invalid branch prefix", formatID: oracleXAFormatID, globalID: []byte("global"), branchID: []byte("1")},
		{name: "invalid branch number", formatID: oracleXAFormatID, globalID: []byte("global"), branchID: []byte("-x")},
		{name: "noncanonical branch number", formatID: oracleXAFormatID, globalID: []byte("global"), branchID: []byte("-01")},
		{name: "branch overflow", formatID: oracleXAFormatID, globalID: []byte("global"), branchID: []byte("-18446744073709551616")},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeOracleXID(test.formatID, test.globalID, test.branchID)
			require.Error(t, err)
		})
	}
}
