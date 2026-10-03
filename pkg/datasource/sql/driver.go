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

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	go_ora "github.com/sijms/go-ora/v2"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/datasource"
	mysql2 "seata.apache.org/seata-go/v2/pkg/datasource/sql/datasource/mysql"
	postgres2 "seata.apache.org/seata-go/v2/pkg/datasource/sql/datasource/postgres"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/util"
	"seata.apache.org/seata-go/v2/pkg/protocol/branch"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

const (
	// SeataATMySQLDriver MySQL driver for AT mode
	SeataATMySQLDriver = "seata-at-mysql"
	// SeataATPostgresDriver PostgreSQL driver for AT mode
	SeataATPostgresDriver = "seata-at-postgres"
	// SeataXAMySQLDriver MySQL driver for XA mode
	SeataXAMySQLDriver = "seata-xa-mysql"
	// SeataXAPostgresDriver PostgreSQL driver for XA mode
	SeataXAPostgresDriver = "seata-xa-postgres"
	// SeataXAOracleDriver Oracle driver for XA mode
	SeataXAOracleDriver = "seata-xa-oracle"
)

type driverDescriptor struct {
	dbType            types.DBType
	target            driver.Driver
	parseDBName       func(dsn string) (string, error)
	resourceID        func(dsn string) string
	newTableMetaCache func(db *sql.DB, dbName string) datasource.TableMetaCache
}

var (
	mySQLDriverDescriptor = driverDescriptor{
		dbType:      types.DBTypeMySQL,
		target:      mysql.MySQLDriver{},
		parseDBName: parseMySQLDBName,
		newTableMetaCache: func(db *sql.DB, dbName string) datasource.TableMetaCache {
			return mysql2.NewTableMetaInstance(db, &mysql.Config{DBName: dbName})
		},
	}
	postgresDriverDescriptor = driverDescriptor{
		dbType:      types.DBTypePostgreSQL,
		target:      stdlib.GetDefaultDriver(),
		parseDBName: parsePostgresDBName,
		newTableMetaCache: func(db *sql.DB, dbName string) datasource.TableMetaCache {
			return postgres2.NewTableMetaInstance(db, dbName)
		},
	}
	oracleDriverDescriptor = driverDescriptor{
		dbType:      types.DBTypeOracle,
		target:      go_ora.GetDefaultDriver(),
		parseDBName: parseOracleDBName,
		resourceID:  parseOracleResourceID,
	}
)

func initDriver() {
	sql.Register(SeataATMySQLDriver, &seataATDriver{
		seataDriver: &seataDriver{
			branchType: branch.BranchTypeAT,
			transType:  types.ATMode,
			target:     mysql.MySQLDriver{},
			targetName: "mysql",
			descriptor: mySQLDriverDescriptor,
		},
	})

	sql.Register(SeataATPostgresDriver, &seataATDriver{
		seataDriver: &seataDriver{
			branchType: branch.BranchTypeAT,
			transType:  types.ATMode,
			descriptor: postgresDriverDescriptor,
		},
	})

	sql.Register(SeataXAMySQLDriver, &seataXADriver{
		seataDriver: &seataDriver{
			branchType: branch.BranchTypeXA,
			transType:  types.XAMode,
			descriptor: mySQLDriverDescriptor,
			target:     mysql.MySQLDriver{},
			targetName: "mysql",
		},
	})

	sql.Register(SeataXAPostgresDriver, &seataXADriver{
		seataDriver: &seataDriver{
			branchType: branch.BranchTypeXA,
			transType:  types.XAMode,
			descriptor: postgresDriverDescriptor,
			target:     stdlib.GetDefaultDriver(),
			targetName: "pgx",
		},
	})

	sql.Register(SeataXAOracleDriver, &seataXADriver{
		seataDriver: &seataDriver{
			branchType: branch.BranchTypeXA,
			transType:  types.XAMode,
			descriptor: oracleDriverDescriptor,
			target:     go_ora.GetDefaultDriver(),
			targetName: "oracle",
		},
	})
}

type seataATDriver struct {
	*seataDriver
}

func (d *seataATDriver) OpenConnector(name string) (c driver.Connector, err error) {
	connector, err := d.seataDriver.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	_connector, _ := connector.(*seataConnector)
	_connector.transType = types.ATMode

	return &seataATConnector{
		seataConnector: _connector,
	}, nil
}

type seataXADriver struct {
	*seataDriver
}

func (d *seataXADriver) OpenConnector(name string) (c driver.Connector, err error) {
	connector, err := d.seataDriver.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	_connector, _ := connector.(*seataConnector)
	_connector.transType = types.XAMode

	return &seataXAConnector{
		seataConnector: _connector,
	}, nil
}

type seataDriver struct {
	branchType branch.BranchType
	transType  types.TransactionMode
	descriptor driverDescriptor
	target     driver.Driver
	targetName string
}

// Open never be called, because seataDriver implemented dri.DriverContext interface.
// reference package: datasource/sql [https://cs.opensource.google/go/go/+/master:src/database/sql/sql.go;l=813]
// and maybe the sql.BD will be call Driver() method, but it obtain the Driver is fron Connector that is proxed by seataConnector.
func (d *seataDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New(("operation unsupport."))
}

func (d *seataDriver) OpenConnector(name string) (c driver.Connector, err error) {
	c = &dsnConnector{dsn: name, driver: d.descriptor.target}
	if driverCtx, ok := d.descriptor.target.(driver.DriverContext); ok {
		c, err = driverCtx.OpenConnector(name)
		if err != nil {
			log.Errorf("open connector: %v", err)
			return nil, err
		}
	}

	dbType := d.descriptor.dbType
	if dbType == types.DBTypeUnknown {
		return nil, fmt.Errorf("unsupport conn type %d", dbType)
	}

	proxy, err := d.getOpenConnectorProxy(c, dbType, sql.OpenDB(c), name)
	if err != nil {
		log.Errorf("register resource: %v", err)
		return nil, err
	}

	return proxy, nil
}

func (d *seataDriver) getOpenConnectorProxy(connector driver.Connector, dbType types.DBType,
	db *sql.DB, dataSourceName string) (driver.Connector, error) {
	meta, err := parseConnectorMetadata(dataSourceName, dbType)
	if err != nil {
		return nil, err
	}

	dbName, err := d.descriptor.parseDBName(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("parse db name: %w", err)
	}
	resourceID := parseResourceID(dataSourceName)
	if d.descriptor.resourceID != nil {
		resourceID = d.descriptor.resourceID(dataSourceName)
	}
	options := []dbOption{
		withResourceID(resourceID),
		withTarget(db),
		withBranchType(d.branchType),
		withDBType(dbType),
		withDBName(dbName),
		withDBName(meta.dbName),
		withConnector(connector),
	}
	res, err := newResource(options...)
	if err != nil {
		log.Errorf("create new resource: %v", err)
		return nil, err
	}

	if d.descriptor.newTableMetaCache != nil {
		datasource.RegisterTableCache(dbType, d.descriptor.newTableMetaCache(db, dbName))
	}
	if err = datasource.GetDataSourceManager(d.branchType).RegisterResource(res); err != nil {
		log.Errorf("register resource: %v", err)
		return nil, err
	}
	return &seataConnector{
		transType:    d.transType,
		branchType:   d.branchType,
		res:          res,
		driver:       d,
		target:       connector,
		targetDriver: d.target,
		targetName:   d.targetName,
		dbType:       dbType,
		dbName:       dbName,
	}, nil
}

func parseMySQLDBName(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	return cfg.DBName, nil
}

func parsePostgresDBName(dsn string) (string, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	return cfg.Database, nil
}

func parseOracleDBName(dsn string) (string, error) {
	cfg, err := go_ora.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("parse oracle dsn: %w", err)
	}
	if cfg.ServiceName != "" {
		return cfg.ServiceName, nil
	}
	if cfg.SID != "" {
		return cfg.SID, nil
	}
	return cfg.DBName, nil
}

// parseOracleResourceID builds a stable service identifier without including
// the username, password, or connection options in the resource ID reported to
// the transaction coordinator.
func parseOracleResourceID(dsn string) string {
	cfg, err := go_ora.ParseConfig(dsn)
	if err == nil {
		addresses := make([]string, 0, len(cfg.Servers))
		for _, server := range cfg.Servers {
			addresses = append(addresses, net.JoinHostPort(server.Addr, strconv.Itoa(server.Port)))
		}
		service := cfg.ServiceName
		if service == "" {
			service = cfg.SID
		}
		if service == "" {
			service = cfg.DBName
		}
		return "oracle://" + strings.Join(addresses, "|") + "/" + service
	}

	// Preserve a useful identifier for malformed DSNs while redacting credentials.
	u, urlErr := url.Parse(dsn)
	if urlErr != nil {
		return "oracle://invalid"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return strings.ReplaceAll(u.String(), ",", "|")
}

func (d *seataDriver) getTargetDriverName() string {
	return d.targetName
}

type connectorMetadata struct {
	dbName string
}

func parseConnectorMetadata(dataSourceName string, dbType types.DBType) (*connectorMetadata, error) {
	switch dbType {
	case types.DBTypeMySQL:
		cfg, err := mysql.ParseDSN(dataSourceName)
		if err != nil {
			return nil, fmt.Errorf("parse mysql dsn: %w", err)
		}
		return &connectorMetadata{dbName: cfg.DBName}, nil
	case types.DBTypePostgreSQL:
		cfg, err := pgx.ParseConfig(dataSourceName)
		if err != nil {
			return nil, fmt.Errorf("parse postgres dsn: %w", err)
		}
		return &connectorMetadata{dbName: cfg.Database}, nil
	case types.DBTypeOracle:
		dbName, err := parseOracleDBName(dataSourceName)
		if err != nil {
			return nil, err
		}
		return &connectorMetadata{dbName: dbName}, nil
	default:
		return nil, fmt.Errorf("unsupported connector metadata for db type %s", dbType.String())
	}
}

type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (t *dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dsn)
}

func (t *dsnConnector) Driver() driver.Driver {
	return t.driver
}

func parseResourceID(dsn string) string {
	i := strings.Index(dsn, "?")
	res := dsn
	if i > 0 {
		res = dsn[:i]
	}
	return strings.ReplaceAll(res, ",", "|")
}

func selectDBVersion(ctx context.Context, conn driver.Conn) (string, error) {
	var rowsi driver.Rows
	var err error

	queryerCtx, ok := conn.(driver.QueryerContext)
	var queryer driver.Queryer
	if !ok {
		queryer, ok = conn.(driver.Queryer)
	}
	if ok {
		rowsi, err = util.CtxDriverQuery(ctx, queryerCtx, queryer, "SELECT VERSION()", nil)
		defer func() {
			if rowsi != nil {
				rowsi.Close()
			}
		}()
		if err != nil {
			log.Errorf("ctx driver query: %+v", err)
			return "", err
		}
	} else {
		log.Errorf("target conn should been driver.QueryerContext or driver.Queryer")
		return "", fmt.Errorf("invalid conn")
	}

	dest := make([]driver.Value, 1)
	var version string
	if err = rowsi.Next(dest); err != nil {
		if err == io.EOF {
			return version, nil
		}
		return "", err
	}
	if len(dest) != 1 {
		return "", errors.New("get db version is not column 1")
	}

	switch reflect.TypeOf(dest[0]).Kind() {
	case reflect.Slice, reflect.Array:
		val := reflect.ValueOf(dest[0]).Bytes()
		version = string(val)
	case reflect.String:
		version = reflect.ValueOf(dest[0]).String()
	default:
		return "", errors.New("get db version is not a string")
	}

	return version, nil
}
