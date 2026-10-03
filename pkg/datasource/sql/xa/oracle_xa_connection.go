/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); You may not use this file except in compliance with
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
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/sijms/go-ora/v2/network"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/util"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

const (
	oracleXAFormatID = 0x53474F
	oracleXAERNOTA   = -4
	oracleXAOK       = 0
	oracleXAReadOnly = 3
)

const (
	oracleXAStartSQL = `DECLARE
	  l_xid DBMS_XA_XID := DBMS_XA_XID(:format_id, HEXTORAW(:gtrid), HEXTORAW(:bqual));
	  l_rc PLS_INTEGER;
	BEGIN
	  l_rc := DBMS_XA.XA_START(l_xid, :flag);
	  :xa_rc := l_rc;
	  IF l_rc NOT IN (0, 3) THEN :oracle_error := DBMS_XA.XA_GETLASTOER; ELSE :oracle_error := 0; END IF;
	END;`
	oracleXAEndSQL = `DECLARE
	  l_xid DBMS_XA_XID := DBMS_XA_XID(:format_id, HEXTORAW(:gtrid), HEXTORAW(:bqual));
	  l_rc PLS_INTEGER;
	BEGIN
	  l_rc := DBMS_XA.XA_END(l_xid, :flag);
	  :xa_rc := l_rc;
	  IF l_rc NOT IN (0, 3) THEN :oracle_error := DBMS_XA.XA_GETLASTOER; ELSE :oracle_error := 0; END IF;
	END;`
	oracleXAPrepareSQL = `DECLARE
	  l_xid DBMS_XA_XID := DBMS_XA_XID(:format_id, HEXTORAW(:gtrid), HEXTORAW(:bqual));
	  l_rc PLS_INTEGER;
	BEGIN
	  l_rc := DBMS_XA.XA_PREPARE(l_xid);
	  :xa_rc := l_rc;
	  IF l_rc NOT IN (0, 3) THEN :oracle_error := DBMS_XA.XA_GETLASTOER; ELSE :oracle_error := 0; END IF;
	END;`
	oracleXACommitSQL = `DECLARE
	  l_xid DBMS_XA_XID := DBMS_XA_XID(:format_id, HEXTORAW(:gtrid), HEXTORAW(:bqual));
	  l_rc PLS_INTEGER;
	BEGIN
	  l_rc := DBMS_XA.XA_COMMIT(l_xid, :one_phase = 1);
	  :xa_rc := l_rc;
	  IF l_rc NOT IN (0, 3) THEN :oracle_error := DBMS_XA.XA_GETLASTOER; ELSE :oracle_error := 0; END IF;
	END;`
	oracleXARollbackSQL = `DECLARE
	  l_xid DBMS_XA_XID := DBMS_XA_XID(:format_id, HEXTORAW(:gtrid), HEXTORAW(:bqual));
	  l_rc PLS_INTEGER;
	BEGIN
	  l_rc := DBMS_XA.XA_ROLLBACK(l_xid);
	  :xa_rc := l_rc;
	  IF l_rc NOT IN (0, 3) THEN :oracle_error := DBMS_XA.XA_GETLASTOER; ELSE :oracle_error := 0; END IF;
	END;`
	oracleXAForgetSQL = `DECLARE
	  l_xid DBMS_XA_XID := DBMS_XA_XID(:format_id, HEXTORAW(:gtrid), HEXTORAW(:bqual));
	  l_rc PLS_INTEGER;
	BEGIN
	  l_rc := DBMS_XA.XA_FORGET(l_xid);
	  :xa_rc := l_rc;
	  IF l_rc NOT IN (0, 3) THEN :oracle_error := DBMS_XA.XA_GETLASTOER; ELSE :oracle_error := 0; END IF;
	END;`
	oracleXARecoverSQL = `SELECT FORMATID, GLOBALID, BRANCHID
	FROM DBA_PENDING_TRANSACTIONS
	WHERE FORMATID = :format_id`
)

var ErrXAReadOnly = errors.New("Oracle XA branch is read-only")

func init() {
	RegisterXAResourceFactory(types.DBTypeOracle, &oracleXAResourceFactory{})
}

type oracleXAResourceFactory struct{}

func (f *oracleXAResourceFactory) CreateXAResource(conn driver.Conn) XAResource {
	return &OracleXAConn{Conn: conn}
}

func (f *oracleXAResourceFactory) CreateErrorClassifier() XAErrorClassifier {
	return &OracleXAErrorClassifier{}
}

// OracleXAError retains both the DBMS_XA return code and the Oracle error code
// returned by XA_GETLASTOER. Cause is populated when the driver itself failed
// to execute the PL/SQL block.
type OracleXAError struct {
	Operation  string
	XID        string
	Code       int64
	OracleCode int64
	Cause      error
}

func (e *OracleXAError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("oracle xa %s failed for xid %q: %v", e.Operation, e.XID, e.Cause)
	}
	return fmt.Sprintf("oracle xa %s failed for xid %q: DBMS_XA code %d, Oracle code %d", e.Operation, e.XID, e.Code, e.OracleCode)
}

func (e *OracleXAError) Unwrap() error { return e.Cause }

// OracleXAErrorClassifier classifies Oracle errors that mean XA_END has
// already detached the branch, allowing the caller to proceed to rollback.
type OracleXAErrorClassifier struct{}

func (c *OracleXAErrorClassifier) IsAlreadyEnded(err error) bool {
	var xaErr *OracleXAError
	if errors.As(err, &xaErr) && xaErr.Operation == "end" {
		if xaErr.Code == oracleXAERNOTA {
			return true
		}
		var oracleErr *network.OracleError
		return errors.As(xaErr.Cause, &oracleErr) && oracleErr.ErrCode == 24756
	}
	return false
}

// OracleXAConn implements XAResource for Oracle through the DBMS_XA PL/SQL
// package. Every call uses the wrapped physical connection so the session
// association created by XA_START remains attached to application SQL.
type OracleXAConn struct {
	driver.Conn
}

func (c *OracleXAConn) Start(ctx context.Context, xid string, flags int) error {
	log.Infof("xa branch start (oracle), xid %s", xid)
	if flags != TMNoFlags && flags != TMJoin && flags != TMResume {
		return fmt.Errorf("oracle xa start: unsupported flags %d", flags)
	}
	return c.callXA(ctx, "start", xid, flags, false)
}

func (c *OracleXAConn) End(ctx context.Context, xid string, flags int) error {
	log.Infof("xa branch end (oracle), xid %s", xid)
	// Oracle DBMS_XA does not support TMFAIL. The branch is detached with
	// TMSUCCESS and the caller follows with XA_ROLLBACK when rolling back.
	if flags == TMFail {
		flags = TMSuccess
	}
	if flags != TMSuccess && flags != TMSuspend {
		return fmt.Errorf("oracle xa end: unsupported flags %d", flags)
	}
	return c.callXA(ctx, "end", xid, flags, false)
}

func (c *OracleXAConn) XAPrepare(ctx context.Context, xid string) error {
	log.Infof("xa branch prepare (oracle), xid %s", xid)
	return c.callXA(ctx, "prepare", xid, 0, false)
}

func (c *OracleXAConn) Commit(ctx context.Context, xid string, onePhase bool) error {
	log.Infof("xa branch commit (oracle), xid %s, one phase %t", xid, onePhase)
	err := c.callXA(ctx, "commit", xid, 0, onePhase)
	if !onePhase && isOracleMissingBranch(err) {
		// A two-phase retry after a committed or XA_RDONLY branch has no work left.
		return nil
	}
	return err
}

func (c *OracleXAConn) Rollback(ctx context.Context, xid string) error {
	log.Infof("xa branch rollback (oracle), xid %s", xid)
	err := c.callXA(ctx, "rollback", xid, 0, false)
	if isOracleMissingBranch(err) {
		// The branch is already absent, which is the required state after rollback.
		return nil
	}
	return err
}

func (c *OracleXAConn) Recover(ctx context.Context, flag int) ([]string, error) {
	allowedFlags := TMStartRScan | TMEndRScan
	if flag&^allowedFlags != 0 {
		return nil, fmt.Errorf("oracle xa recover: unsupported flags %d", flag)
	}
	if flag&TMStartRScan == 0 {
		return nil, nil
	}

	rows, err := util.CtxDriverQueryWithPrepareFallback(ctx, c.Conn, oracleXARecoverSQL, []driver.NamedValue{{
		Ordinal: 1,
		Name:    "format_id",
		Value:   int64(oracleXAFormatID),
	}})
	if err != nil {
		return nil, fmt.Errorf("oracle xa recover query DBA_PENDING_TRANSACTIONS: %w", err)
	}

	xids, scanErr := scanOraclePendingTransactions(rows)
	closeErr := rows.Close()
	if scanErr != nil || closeErr != nil {
		return nil, errors.Join(scanErr, closeErr)
	}
	return xids, nil
}

func scanOraclePendingTransactions(rows driver.Rows) ([]string, error) {
	xids := make([]string, 0)
	dest := make([]driver.Value, 3)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return xids, nil
			}
			return nil, fmt.Errorf("read Oracle pending transaction row: %w", err)
		}

		formatID, err := oracleNumber(dest[0])
		if err != nil {
			return nil, fmt.Errorf("read Oracle pending transaction format ID: %w", err)
		}
		if formatID != oracleXAFormatID {
			continue
		}
		globalID, err := oracleRaw(dest[1])
		if err != nil {
			return nil, fmt.Errorf("read Oracle pending transaction global ID: %w", err)
		}
		branchID, err := oracleRaw(dest[2])
		if err != nil {
			return nil, fmt.Errorf("read Oracle pending transaction branch ID: %w", err)
		}
		xid, err := decodeOracleXID(formatID, globalID, branchID)
		if err != nil {
			return nil, fmt.Errorf("decode Oracle pending transaction XID: %w", err)
		}
		xids = append(xids, xid)
	}
}

func oracleNumber(value driver.Value) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		if float64(int64(v)) == v {
			return int64(v), nil
		}
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	}
	return 0, fmt.Errorf("unexpected Oracle NUMBER value type %T", value)
}

func oracleRaw(value driver.Value) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unexpected Oracle RAW value type %T", value)
	}
}

func (c *OracleXAConn) Forget(ctx context.Context, xid string) error {
	return c.callXA(ctx, "forget", xid, 0, false)
}

func (c *OracleXAConn) GetTransactionTimeout() time.Duration { return 0 }

func (c *OracleXAConn) IsSameRM(ctx context.Context, resource XAResource) bool { return false }

func (c *OracleXAConn) SetTransactionTimeout(duration time.Duration) bool { return false }

func (c *OracleXAConn) callXA(ctx context.Context, operation, xid string, flags int, onePhase bool) error {
	oracleXID, err := parseOracleXID(xid)
	if err != nil {
		return fmt.Errorf("oracle xa %s: %w", operation, err)
	}

	query, ok := map[string]string{
		"start":    oracleXAStartSQL,
		"end":      oracleXAEndSQL,
		"prepare":  oracleXAPrepareSQL,
		"commit":   oracleXACommitSQL,
		"rollback": oracleXARollbackSQL,
		"forget":   oracleXAForgetSQL,
	}[operation]
	if !ok {
		return fmt.Errorf("unsupported Oracle XA operation %q", operation)
	}

	var xaCode, oracleCode int64
	args := []driver.NamedValue{
		{Ordinal: 1, Name: "format_id", Value: int64(oracleXID.FormatID)},
		{Ordinal: 2, Name: "gtrid", Value: oracleXID.GlobalIDHex},
		{Ordinal: 3, Name: "bqual", Value: oracleXID.BranchIDHex},
	}
	switch operation {
	case "start", "end":
		args = append(args, driver.NamedValue{Ordinal: len(args) + 1, Name: "flag", Value: int64(flags)})
	case "commit":
		args = append(args, driver.NamedValue{Ordinal: len(args) + 1, Name: "one_phase", Value: int64(boolToInt(onePhase))})
	}
	args = append(args,
		driver.NamedValue{Ordinal: len(args) + 1, Name: "xa_rc", Value: sql.Out{Dest: &xaCode}},
		driver.NamedValue{Ordinal: len(args) + 2, Name: "oracle_error", Value: sql.Out{Dest: &oracleCode}},
	)
	if _, err = util.CtxDriverExecWithPrepareFallback(ctx, c.Conn, query, args); err != nil {
		return &OracleXAError{Operation: operation, XID: xid, Cause: err}
	}
	if operation == "prepare" && xaCode == oracleXAReadOnly {
		return ErrXAReadOnly
	}
	if xaCode != oracleXAOK {
		return &OracleXAError{Operation: operation, XID: xid, Code: xaCode, OracleCode: oracleCode}
	}
	return nil
}

func isOracleMissingBranch(err error) bool {
	var xaErr *OracleXAError
	if !errors.As(err, &xaErr) {
		return false
	}
	if xaErr.Code == oracleXAERNOTA {
		return true
	}
	var oracleErr *network.OracleError
	return errors.As(xaErr.Cause, &oracleErr) && oracleErr.ErrCode == 24756
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

const oracleXIDFieldMaxBytes = 64

// oracleXID is the validated mapping between Seata's string XID and the Oracle
// DBMS_XA_XID fields. Hex strings are used only as bind values for HEXTORAW.
type oracleXID struct {
	FormatID    int64
	GlobalID    []byte
	BranchID    []byte
	GlobalIDHex string
	BranchIDHex string
}

func parseOracleXID(xid string) (*oracleXID, error) {
	if xid == "" {
		return nil, fmt.Errorf("empty XID")
	}
	separator := strings.LastIndexByte(xid, '-')
	if separator <= 0 || separator == len(xid)-1 {
		return nil, fmt.Errorf("XID %q must end with -<positive branch ID>", xid)
	}

	globalID := []byte(xid[:separator])
	branchQualifier := []byte(xid[separator:])
	if len(globalID) == 0 || len(globalID) > oracleXIDFieldMaxBytes {
		return nil, fmt.Errorf("Oracle global transaction ID length %d is outside 1..%d bytes", len(globalID), oracleXIDFieldMaxBytes)
	}
	if len(branchQualifier) > oracleXIDFieldMaxBytes {
		return nil, fmt.Errorf("Oracle branch qualifier length %d exceeds %d bytes", len(branchQualifier), oracleXIDFieldMaxBytes)
	}
	branchID, err := parseOracleBranchQualifier(branchQualifier)
	if err != nil {
		return nil, err
	}
	if strconv.FormatUint(branchID, 10) != string(branchQualifier[1:]) {
		return nil, fmt.Errorf("non-canonical Oracle branch qualifier %q", branchQualifier)
	}

	return &oracleXID{
		FormatID:    oracleXAFormatID,
		GlobalID:    globalID,
		BranchID:    branchQualifier,
		GlobalIDHex: hex.EncodeToString(globalID),
		BranchIDHex: hex.EncodeToString(branchQualifier),
	}, nil
}

func decodeOracleXID(formatID int64, globalID, branchQualifier []byte) (string, error) {
	if formatID != oracleXAFormatID {
		return "", fmt.Errorf("unsupported Oracle XA format ID %d", formatID)
	}
	if len(globalID) == 0 || len(globalID) > oracleXIDFieldMaxBytes {
		return "", fmt.Errorf("Oracle global transaction ID length %d is outside 1..%d bytes", len(globalID), oracleXIDFieldMaxBytes)
	}
	if len(branchQualifier) == 0 || len(branchQualifier) > oracleXIDFieldMaxBytes {
		return "", fmt.Errorf("Oracle branch qualifier length %d is outside 1..%d bytes", len(branchQualifier), oracleXIDFieldMaxBytes)
	}
	branchID, err := parseOracleBranchQualifier(branchQualifier)
	if err != nil {
		return "", err
	}
	if strconv.FormatUint(branchID, 10) != string(branchQualifier[1:]) {
		return "", fmt.Errorf("non-canonical Oracle branch qualifier %q", branchQualifier)
	}
	return string(globalID) + string(branchQualifier), nil
}

func parseOracleBranchQualifier(branchQualifier []byte) (uint64, error) {
	if len(branchQualifier) < 2 || branchQualifier[0] != '-' {
		return 0, fmt.Errorf("invalid Oracle branch qualifier %q: expected -<positive branch ID>", branchQualifier)
	}
	for _, digit := range branchQualifier[1:] {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("invalid Oracle branch qualifier %q: branch ID must be decimal", branchQualifier)
		}
	}
	branchID, err := strconv.ParseUint(string(branchQualifier[1:]), 10, 64)
	if err != nil || branchID == 0 {
		return 0, fmt.Errorf("invalid Oracle branch qualifier %q: branch ID must be a positive uint64", branchQualifier)
	}
	return branchID, nil
}
