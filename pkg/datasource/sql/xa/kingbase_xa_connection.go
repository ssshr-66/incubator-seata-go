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
	"strings"
	"time"
	"unicode/utf8"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/util"
)

const (
	kingbaseGIDPrefix  = "seata-go:kingbase:"
	kingbaseMaxGIDLen  = 199
	kingbaseRecoverSQL = "SELECT gid FROM sys_prepared_xacts WHERE database = current_database()"
)

func init() {
	RegisterXAResourceFactory(types.DBTypeKingbase, &kingbaseXAResourceFactory{})
}

type kingbaseXAResourceFactory struct{}

func (f *kingbaseXAResourceFactory) CreateXAResource(conn driver.Conn) XAResource {
	return &KingbaseXAConn{Conn: conn}
}

func (f *kingbaseXAResourceFactory) CreateErrorClassifier() XAErrorClassifier {
	return &KingbaseXAErrorClassifier{}
}

// KingbaseXAErrorClassifier does not treat unverified server errors as proof
// that a prepared transaction has already completed.
type KingbaseXAErrorClassifier struct{}

func (c *KingbaseXAErrorClassifier) IsAlreadyEnded(error) bool { return false }

// KingbaseXAConn implements XAResource with KingbaseES prepared transactions.
// The GID prefix keeps this adapter's recover scan separate from unrelated
// prepared transactions in the same database.
type KingbaseXAConn struct {
	driver.Conn
	tx        driver.Tx
	activeXID string
}

func (c *KingbaseXAConn) Start(ctx context.Context, xid string, flags int) error {
	if flags != TMNoFlags {
		return errors.New("invalid arguments")
	}
	if c.tx != nil {
		return fmt.Errorf("kingbase xa transaction already started, xid %s", xid)
	}
	if _, err := kingbaseGID(xid); err != nil {
		return err
	}

	var (
		tx  driver.Tx
		err error
	)
	if conn, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = conn.BeginTx(ctx, driver.TxOptions{})
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return fmt.Errorf("kingbase xa begin: %w", err)
	}
	c.tx = tx
	c.activeXID = xid
	return nil
}

func (c *KingbaseXAConn) End(_ context.Context, xid string, flags int) error {
	if c.tx == nil || c.activeXID != xid {
		return fmt.Errorf("kingbase xa end requires the active transaction, xid %s", xid)
	}
	switch flags {
	case TMSuccess, TMFail:
		// PREPARE TRANSACTION ends the session's association with this branch.
		return nil
	default:
		return errors.New("invalid arguments")
	}
}

func (c *KingbaseXAConn) XAPrepare(ctx context.Context, xid string) error {
	if c.tx == nil || c.activeXID != xid {
		return fmt.Errorf("kingbase xa prepare requires the active transaction, xid %s", xid)
	}
	quotedGID, err := quoteKingbaseGID(xid)
	if err != nil {
		return err
	}
	if err = kingbaseExec(ctx, c.Conn, "PREPARE TRANSACTION "+quotedGID); err != nil {
		return fmt.Errorf("kingbase xa prepare failed, xid %s: %w", xid, err)
	}
	c.tx = nil
	c.activeXID = ""
	return nil
}

func (c *KingbaseXAConn) Commit(ctx context.Context, xid string, onePhase bool) error {
	if onePhase {
		if c.tx == nil || c.activeXID != xid {
			return fmt.Errorf("kingbase xa one-phase commit requires the active transaction, xid %s", xid)
		}
		if err := c.tx.Commit(); err != nil {
			return fmt.Errorf("kingbase xa one-phase commit failed, xid %s: %w", xid, err)
		}
		c.tx = nil
		c.activeXID = ""
		return nil
	}
	if c.tx != nil {
		return fmt.Errorf("kingbase xa two-phase commit requires a prepared transaction, xid %s", xid)
	}
	quotedGID, err := quoteKingbaseGID(xid)
	if err != nil {
		return err
	}
	if err = kingbaseExec(ctx, c.Conn, "COMMIT PREPARED "+quotedGID); err != nil {
		return fmt.Errorf("kingbase xa commit prepared failed, xid %s: %w", xid, err)
	}
	return nil
}

func (c *KingbaseXAConn) Rollback(ctx context.Context, xid string) error {
	if c.tx != nil {
		if c.activeXID != xid {
			return fmt.Errorf("kingbase xa rollback xid does not match the active transaction")
		}
		if err := c.tx.Rollback(); err != nil {
			return fmt.Errorf("kingbase xa rollback active transaction failed, xid %s: %w", xid, err)
		}
		c.tx = nil
		c.activeXID = ""
		return nil
	}
	quotedGID, err := quoteKingbaseGID(xid)
	if err != nil {
		return err
	}
	if err = kingbaseExec(ctx, c.Conn, "ROLLBACK PREPARED "+quotedGID); err != nil {
		return fmt.Errorf("kingbase xa rollback prepared failed, xid %s: %w", xid, err)
	}
	return nil
}

func (c *KingbaseXAConn) Recover(ctx context.Context, flag int) (xids []string, err error) {
	const supportedFlags = TMStartRScan | TMEndRScan
	if flag&^supportedFlags != 0 {
		return nil, errors.New("invalid arguments")
	}
	if flag&TMStartRScan == 0 {
		return nil, nil
	}

	rows, err := kingbaseQuery(ctx, c.Conn, kingbaseRecoverSQL)
	if err != nil {
		return nil, fmt.Errorf("kingbase xa recover query failed: %w", err)
	}
	if rows == nil {
		return nil, errors.New("kingbase xa recover query returned no rows handle")
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			xids = nil
			err = fmt.Errorf("kingbase xa recover close rows: %w", closeErr)
		}
	}()

	xids = make([]string, 0)
	dest := make([]driver.Value, 1)
	for {
		dest[0] = nil
		if err = rows.Next(dest); err != nil {
			if err == io.EOF {
				return xids, nil
			}
			return nil, fmt.Errorf("kingbase xa recover read rows: %w", err)
		}
		if len(dest) != 1 || dest[0] == nil {
			return nil, errors.New("kingbase xa recover returned an invalid GID row")
		}

		var gid string
		switch value := dest[0].(type) {
		case string:
			gid = value
		case []byte:
			gid = string(value)
		default:
			return nil, fmt.Errorf("kingbase xa recover returned unsupported GID type %T", dest[0])
		}
		if gid == "" {
			return nil, errors.New("kingbase xa recover returned an empty GID")
		}
		xid, owned, decodeErr := decodeKingbaseGID(gid)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if owned {
			xids = append(xids, xid)
		}
	}
}

func (c *KingbaseXAConn) Forget(_ context.Context, _ string) error {
	return errors.New("KingbaseES does not support XA forget")
}

func (c *KingbaseXAConn) GetTransactionTimeout() time.Duration { return 0 }

func (c *KingbaseXAConn) IsSameRM(context.Context, XAResource) bool { return false }

func (c *KingbaseXAConn) SetTransactionTimeout(time.Duration) bool { return false }

func kingbaseGID(xid string) (string, error) {
	if xid == "" {
		return "", errors.New("kingbase xa XID must not be empty")
	}
	if !utf8.ValidString(xid) {
		return "", errors.New("kingbase xa XID must be valid UTF-8")
	}
	if strings.ContainsAny(xid, "\\\x00") {
		return "", errors.New("kingbase xa XID must not contain backslash or NUL")
	}
	gid := kingbaseGIDPrefix + xid
	if len(gid) > kingbaseMaxGIDLen {
		return "", fmt.Errorf("kingbase xa GID must be at most %d bytes", kingbaseMaxGIDLen)
	}
	return gid, nil
}

func quoteKingbaseGID(xid string) (string, error) {
	gid, err := kingbaseGID(xid)
	if err != nil {
		return "", err
	}
	return "'" + strings.ReplaceAll(gid, "'", "''") + "'", nil
}

func decodeKingbaseGID(gid string) (string, bool, error) {
	if !strings.HasPrefix(gid, kingbaseGIDPrefix) {
		return "", false, nil
	}
	xid := strings.TrimPrefix(gid, kingbaseGIDPrefix)
	encoded, err := kingbaseGID(xid)
	if err != nil {
		return "", true, fmt.Errorf("kingbase xa recover found invalid Seata GID: %w", err)
	}
	if encoded != gid {
		return "", true, errors.New("kingbase xa recover found a non-canonical Seata GID")
	}
	return xid, true, nil
}

func kingbaseExec(ctx context.Context, conn driver.Conn, query string) error {
	if execer, ok := conn.(driver.ExecerContext); ok {
		_, err := execer.ExecContext(ctx, query, nil)
		return err
	}
	if execer, ok := conn.(driver.Execer); ok {
		_, err := execer.Exec(query, nil)
		return err
	}
	return errors.New("kingbase xa connection does not support direct execution")
}

func kingbaseQuery(ctx context.Context, conn driver.Conn, query string) (driver.Rows, error) {
	queryerCtx, hasQueryerCtx := conn.(driver.QueryerContext)
	queryer, hasQueryer := conn.(driver.Queryer)
	if !hasQueryerCtx && !hasQueryer {
		return nil, errors.New("kingbase xa connection does not support direct query")
	}
	return util.CtxDriverQuery(ctx, queryerCtx, queryer, query, nil)
}
