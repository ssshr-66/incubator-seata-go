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
	"fmt"
	"reflect"
	"testing"

	"github.com/bluele/gcache"
	"seata.apache.org/seata-go/v2/pkg/rm"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/mock"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/xa"
	"seata.apache.org/seata-go/v2/pkg/protocol/branch"
	"seata.apache.org/seata-go/v2/pkg/tm"
	"seata.apache.org/seata-go/v2/pkg/util/reflectx"
)

func initMockResourceManager(branchType branch.BranchType, ctrl *gomock.Controller) *mock.MockDataSourceManager {
	mockResourceMgr := mock.NewMockDataSourceManager(ctrl)
	mockResourceMgr.SetBranchType(branchType)
	mockResourceMgr.EXPECT().BranchRegister(gomock.Any(), gomock.Any()).AnyTimes().Return(int64(0), nil)
	rm.GetRmCacheInstance().RegisterResourceManager(mockResourceMgr)
	mockResourceMgr.EXPECT().RegisterResource(gomock.Any()).AnyTimes().Return(nil)
	mockResourceMgr.EXPECT().CreateTableMetaCache(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)

	return mockResourceMgr
}

func Test_seataATDriver_Open(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := initMockResourceManager(branch.BranchTypeAT, ctrl)
	_ = mockMgr

	db, err := sql.Open("seata-at-mysql", "root:seata_go@tcp(127.0.0.1:3306)/seata_go_test?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	_ = initMockAtConnector(t, ctrl, db, func(t *testing.T, ctrl *gomock.Controller) driver.Connector {
		connector := mock.NewMockTestDriverConnector(ctrl)
		connector.EXPECT().Connect(gomock.Any()).Return(nil, fmt.Errorf("connect error"))
		return connector
	})

	conn, err := db.Conn(context.Background())
	assert.NotNil(t, err)
	assert.Nil(t, conn)
}

func Test_seataATDriver_OpenConnector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := initMockResourceManager(branch.BranchTypeAT, ctrl)
	_ = mockMgr

	db, err := sql.Open("seata-at-mysql", "root:seata_go@tcp(127.0.0.1:3306)/seata_go_test?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	v := reflect.ValueOf(db)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	field := v.FieldByName("connector")
	fieldVal := reflectx.GetUnexportedField(field)

	_, ok := fieldVal.(*seataATConnector)
	assert.True(t, ok, "need return seata at connector")
}

func Test_seataATPostgresDriver_OpenConnector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := initMockResourceManager(branch.BranchTypeAT, ctrl)
	_ = mockMgr

	db, err := sql.Open(SeataATPostgresDriver, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	v := reflect.ValueOf(db)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	field := v.FieldByName("connector")
	fieldVal := reflectx.GetUnexportedField(field)

	connector, ok := fieldVal.(*seataATConnector)
	assert.True(t, ok, "need return seata at connector")
	assert.Equal(t, types.DBTypePostgreSQL, connector.dbType)
	assert.Equal(t, "seata_go_test", connector.dbName)
}

func Test_seataXADriver_OpenConnector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := initMockResourceManager(branch.BranchTypeXA, ctrl)
	_ = mockMgr

	db, err := sql.Open("seata-xa-mysql", "root:seata_go@tcp(127.0.0.1:3306)/seata_go_test?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	v := reflect.ValueOf(db)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	field := v.FieldByName("connector")
	fieldVal := reflectx.GetUnexportedField(field)

	_, ok := fieldVal.(*seataXAConnector)
	assert.True(t, ok, "need return seata xa connector")
}

func TestSeataXAMariaDBDriverRegistered(t *testing.T) {
	db, err := sql.Open(SeataXAMariaDBDriver, "user:pass@tcp(127.0.0.1:3306)missing_slash")
	assert.Nil(t, db)
	assert.ErrorContains(t, err, "invalid DSN")
}

func TestParseConnectorMetadataMariaDB(t *testing.T) {
	meta, err := parseConnectorMetadata("user:pass@tcp(127.0.0.1:3306)/seata_demo", types.DBTypeMARIADB)
	assert.NoError(t, err)
	if assert.NotNil(t, meta) {
		assert.Equal(t, "seata_demo", meta.dbName)
	}
}

func TestSeataXAMariaDBConnectorAndResourceDBType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	initMockResourceManager(branch.BranchTypeXA, ctrl)

	mockConn := mock.NewMockTestDriverConn(ctrl)
	mockConnector := mock.NewMockTestDriverConnector(ctrl)
	mockRows := mock.NewMockTestDriverRows(ctrl)
	mockConnector.EXPECT().Connect(gomock.Any()).Return(mockConn, nil).Times(2)
	mockConn.EXPECT().QueryContext(gomock.Any(), "SELECT VERSION()", gomock.Any()).Return(mockRows, nil)
	mockConn.EXPECT().ExecContext(gomock.Any(), "XA START 'mariadb-xid-0'", gomock.Any()).Return(driver.RowsAffected(0), nil)
	mockConn.EXPECT().ExecContext(gomock.Any(), "XA END 'mariadb-xid-0'", gomock.Any()).Return(driver.RowsAffected(0), nil)
	mockConn.EXPECT().ExecContext(gomock.Any(), "XA ROLLBACK 'mariadb-xid-0'", gomock.Any()).Return(driver.RowsAffected(0), nil)
	mockConn.EXPECT().Close().Return(nil).Times(2)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "10.11.6-MariaDB"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	dsn := "user:pass@tcp(127.0.0.1:3306)/seata_demo"
	db := sql.OpenDB(mockConnector)
	defer db.Close()

	seataDriver := &seataDriver{
		branchType: branch.BranchTypeXA,
		transType:  types.XAMode,
		descriptor: mariaDBDriverDescriptor,
		targetName: "mysql",
	}
	connector, err := seataDriver.getOpenConnectorProxy(mockConnector, types.DBTypeMARIADB, db, dsn)
	if !assert.NoError(t, err) {
		return
	}

	seataXAConnector := &seataXAConnector{seataConnector: connector.(*seataConnector)}
	assert.Equal(t, types.DBTypeMARIADB, seataXAConnector.dbType)
	assert.Equal(t, "seata_demo", seataXAConnector.dbName)
	assert.Equal(t, types.DBTypeMARIADB, seataXAConnector.res.GetDbType())
	assert.Equal(t, "seata_demo", seataXAConnector.res.GetDBName())
	assert.True(t, seataXAConnector.res.IsShouldBeHeld(), "MariaDB XA resources must retain the phase-one connection")

	conn, err := seataXAConnector.Connect(context.Background())
	if assert.NoError(t, err) {
		xaConn, ok := conn.(*XAConn)
		if assert.True(t, ok) {
			assert.Equal(t, types.DBTypeMARIADB, xaConn.dbType)
			assert.Equal(t, "seata_demo", xaConn.dbName)

			previousBranchStatusCache := branchStatusCache
			t.Cleanup(func() { branchStatusCache = previousBranchStatusCache })
			branchStatusCache = gcache.New(1024).LRU().Build()
			globalCtx := tm.InitSeataContext(context.Background())
			tm.SetXID(globalCtx, "mariadb-xid")
			xaTx, beginErr := xaConn.BeginTx(globalCtx, driver.TxOptions{})
			if assert.NoError(t, beginErr) {
				assert.IsType(t, &xa.MariaDBXAConn{}, xaConn.xaResource)
				assert.IsType(t, &xa.MariaDBXAErrorClassifier{}, xaConn.xaErrorClassifier)
				keptConn, exists := seataXAConnector.res.Lookup("mariadb-xid-0")
				assert.True(t, exists)
				assert.Same(t, xaConn, keptConn)

				assert.NoError(t, xaTx.Rollback())
				_, exists = seataXAConnector.res.Lookup("mariadb-xid-0")
				assert.False(t, exists, "rollback should release the MariaDB phase-one connection")
				assert.NoError(t, xaConn.Close())
			}
		}
	}
}
