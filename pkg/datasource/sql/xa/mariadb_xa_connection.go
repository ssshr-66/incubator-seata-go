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
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
)

const mariaDBMaxXAIDComponentLength = 64

func init() {
	RegisterXAResourceFactory(types.DBTypeMARIADB, &mariaDBXAResourceFactory{})
}

type mariaDBXAResourceFactory struct{}

func (*mariaDBXAResourceFactory) CreateXAResource(conn driver.Conn) XAResource {
	return &MariaDBXAConn{Conn: conn}
}

func (*mariaDBXAResourceFactory) CreateErrorClassifier() XAErrorClassifier {
	return &MariaDBXAErrorClassifier{}
}

// MariaDBXAErrorClassifier classifies MariaDB XA errors that mean a branch has
// already left a state in which XA END is legal.
type MariaDBXAErrorClassifier struct{}

func (*MariaDBXAErrorClassifier) IsAlreadyEnded(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != types.ErrCodeXAER_RMFAIL_IDLE {
		return false
	}

	message := strings.ToLower(mysqlErr.Message)
	return strings.Contains(message, "idle state") ||
		strings.Contains(message, "prepared state") ||
		strings.Contains(message, "already ended")
}

// MariaDBXAConn implements XAResource using MariaDB's external XA statements.
type MariaDBXAConn struct {
	driver.Conn
}

func (c *MariaDBXAConn) Start(ctx context.Context, xid string, flags int) error {
	if flags != TMNoFlags {
		return errors.New("mariadb xa start only supports TMNoFlags")
	}
	xidSQL, err := mariaDBXIDSQL(xid)
	if err != nil {
		return err
	}
	return c.exec(ctx, "XA START "+xidSQL)
}

func (c *MariaDBXAConn) End(ctx context.Context, xid string, flags int) error {
	if flags != TMSuccess && flags != TMFail {
		return errors.New("mariadb xa end only supports TMSuccess and TMFail")
	}
	xidSQL, err := mariaDBXIDSQL(xid)
	if err != nil {
		return err
	}
	return c.exec(ctx, "XA END "+xidSQL)
}

func (c *MariaDBXAConn) XAPrepare(ctx context.Context, xid string) error {
	xidSQL, err := mariaDBXIDSQL(xid)
	if err != nil {
		return err
	}
	return c.exec(ctx, "XA PREPARE "+xidSQL)
}

func (c *MariaDBXAConn) Commit(ctx context.Context, xid string, onePhase bool) error {
	xidSQL, err := mariaDBXIDSQL(xid)
	if err != nil {
		return err
	}
	query := "XA COMMIT " + xidSQL
	if onePhase {
		query += " ONE PHASE"
	}
	return c.exec(ctx, query)
}

func (c *MariaDBXAConn) Rollback(ctx context.Context, xid string) error {
	xidSQL, err := mariaDBXIDSQL(xid)
	if err != nil {
		return err
	}
	return c.exec(ctx, "XA ROLLBACK "+xidSQL)
}

func (*MariaDBXAConn) Forget(context.Context, string) error {
	return errors.New("mariadb does not support XA FORGET")
}

func (*MariaDBXAConn) GetTransactionTimeout() time.Duration { return 0 }

func (*MariaDBXAConn) IsSameRM(context.Context, XAResource) bool { return false }

func (*MariaDBXAConn) SetTransactionTimeout(time.Duration) bool { return false }

func (c *MariaDBXAConn) Recover(ctx context.Context, flag int) ([]string, error) {
	if flag & ^(TMStartRScan|TMEndRScan) != 0 {
		return nil, errors.New("mariadb xa recover received unsupported flags")
	}
	if flag&TMStartRScan == 0 {
		return nil, nil
	}
	if c == nil || c.Conn == nil {
		return nil, errors.New("mariadb xa recover has no driver connection")
	}
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New("mariadb xa driver connection does not implement QueryerContext")
	}
	rows, err := queryer.QueryContext(ctx, "XA RECOVER", nil)
	if err != nil {
		return nil, fmt.Errorf("execute mariadb XA RECOVER: %w", err)
	}
	if rows == nil {
		return nil, errors.New("mariadb XA RECOVER returned nil rows")
	}
	if len(rows.Columns()) != 4 {
		return nil, closeRowsWithError(rows, errors.New("mariadb XA RECOVER returned an unexpected column count"))
	}

	xids := make([]string, 0)
	for {
		values := make([]driver.Value, 4)
		if err = rows.Next(values); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, closeRowsWithError(rows, fmt.Errorf("read mariadb XA RECOVER rows: %w", err))
		}
		xid, parseErr := parseMariaDBRecoverXID(values)
		if parseErr != nil {
			return nil, closeRowsWithError(rows, parseErr)
		}
		xids = append(xids, xid)
	}
	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("close mariadb XA RECOVER rows: %w", err)
	}
	return xids, nil
}

func (c *MariaDBXAConn) exec(ctx context.Context, query string) error {
	if c == nil || c.Conn == nil {
		return errors.New("mariadb xa operation has no driver connection")
	}
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return errors.New("mariadb xa driver connection does not implement ExecerContext")
	}
	if _, err := execer.ExecContext(ctx, query, nil); err != nil {
		return err
	}
	return nil
}

func mariaDBXIDSQL(xid string) (string, error) {
	if xid == "" {
		return "", errors.New("mariadb xa xid must not be empty")
	}
	if !utf8.ValidString(xid) {
		return "", errors.New("mariadb xa xid must be valid UTF-8")
	}

	for _, char := range xid {
		if char == '\\' || unicode.IsControl(char) {
			return "", errors.New("mariadb xa xid contains an unsupported character")
		}
	}
	if len(xid) <= mariaDBMaxXAIDComponentLength {
		return quoteMariaDBXIDComponent(xid), nil
	}

	// XABranchXid.String() appends "-<branchID>" to the global XID. When
	// that combined string exceeds MariaDB's 64-byte gtrid limit, keep the
	// separator at the start of bqual so XA RECOVER's concatenated data
	// round-trips back to the exact Seata branch XID.
	separator := strings.LastIndexByte(xid, '-')
	if separator <= 0 || separator == len(xid)-1 {
		return "", fmt.Errorf("mariadb xa xid exceeds the %d-byte gtrid limit and has no Seata branch suffix", mariaDBMaxXAIDComponentLength)
	}
	for _, char := range xid[separator+1:] {
		if char < '0' || char > '9' {
			return "", fmt.Errorf("mariadb xa xid exceeds the %d-byte gtrid limit and has a non-numeric branch suffix", mariaDBMaxXAIDComponentLength)
		}
	}

	gtrid := xid[:separator]
	bqual := xid[separator:]
	if len(gtrid) > mariaDBMaxXAIDComponentLength || len(bqual) > mariaDBMaxXAIDComponentLength {
		return "", fmt.Errorf("mariadb xa xid gtrid and bqual must each fit within %d bytes", mariaDBMaxXAIDComponentLength)
	}
	return quoteMariaDBXIDComponent(gtrid) + "," + quoteMariaDBXIDComponent(bqual), nil
}

func quoteMariaDBXIDComponent(component string) string {
	var literal strings.Builder
	literal.Grow(len(component) + 2)
	literal.WriteByte('\'')
	for _, char := range component {
		if char == '\'' {
			literal.WriteString("''")
			continue
		}
		literal.WriteRune(char)
	}
	literal.WriteByte('\'')
	return literal.String()
}

func parseMariaDBRecoverXID(row []driver.Value) (string, error) {
	if len(row) != 4 {
		return "", fmt.Errorf("mariadb XA RECOVER row has %d columns, want 4", len(row))
	}
	formatID, err := mariaDBRecoverInteger(row[0])
	if err != nil || formatID < 0 {
		return "", errors.New("mariadb XA RECOVER row has an invalid formatID")
	}
	gtridLength, err := mariaDBRecoverInteger(row[1])
	if err != nil {
		return "", errors.New("mariadb XA RECOVER row has an invalid gtrid_length")
	}
	bqualLength, err := mariaDBRecoverInteger(row[2])
	if err != nil {
		return "", errors.New("mariadb XA RECOVER row has an invalid bqual_length")
	}
	if gtridLength <= 0 || gtridLength > mariaDBMaxXAIDComponentLength ||
		bqualLength < 0 || bqualLength > mariaDBMaxXAIDComponentLength {
		return "", errors.New("mariadb XA RECOVER row has an out-of-range XID length")
	}
	data, ok := mariaDBRecoverData(row[3])
	if !ok {
		return "", fmt.Errorf("mariadb XA RECOVER data has unsupported type %T", row[3])
	}
	if int64(len(data)) != gtridLength+bqualLength {
		return "", fmt.Errorf("mariadb XA RECOVER data length is %d, want %d", len(data), gtridLength+bqualLength)
	}
	if !utf8.Valid(data) {
		return "", errors.New("mariadb XA RECOVER data is not valid UTF-8")
	}
	return string(data), nil
}

func mariaDBRecoverInteger(value driver.Value) (int64, error) {
	switch value := value.(type) {
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return 0, errors.New("integer overflows int64")
		}
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return 0, errors.New("integer overflows int64")
		}
		return int64(value), nil
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}

func mariaDBRecoverData(value driver.Value) ([]byte, bool) {
	switch value := value.(type) {
	case []byte:
		return value, true
	case string:
		return []byte(value), true
	default:
		return nil, false
	}
}

func closeRowsWithError(rows driver.Rows, err error) error {
	if closeErr := rows.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("close mariadb XA RECOVER rows: %w", closeErr))
	}
	return err
}
