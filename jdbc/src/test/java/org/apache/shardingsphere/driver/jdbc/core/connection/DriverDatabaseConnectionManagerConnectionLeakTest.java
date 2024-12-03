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

package org.apache.shardingsphere.driver.jdbc.core.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.infra.database.core.DefaultDatabase;
import org.apache.shardingsphere.infra.datasource.pool.props.domain.DataSourcePoolProperties;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.metadata.persist.MetaDataPersistService;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.mockito.Mockito.*;

class DriverDatabaseConnectionManagerConnectionLeakTest {

    private DriverDatabaseConnectionManager databaseConnectionManager;

    private HikariDataSource spyDs;

    @BeforeEach
    void setUp() throws SQLException {
        spyDs = createDataSource();
        databaseConnectionManager = new DriverDatabaseConnectionManager(DefaultDatabase.LOGIC_NAME, mockContextManager());
    }

    private ContextManager mockContextManager() throws SQLException {
        ContextManager result = mock(ContextManager.class, RETURNS_DEEP_STUBS);
        Map<String, StorageUnit> storageUnits = mockStorageUnits();
        when(result.getStorageUnits(DefaultDatabase.LOGIC_NAME)).thenReturn(storageUnits);
        MetaDataPersistService persistService = mockMetaDataPersistService();
        when(result.getPersistServiceFacade().getMetaDataPersistService()).thenReturn(persistService);
        when(result.getMetaDataContexts().getMetaData().getGlobalRuleMetaData()).thenReturn(new RuleMetaData(Collections.singleton(mock(TransactionRule.class, RETURNS_DEEP_STUBS))));
        return result;
    }

    private Map<String, StorageUnit> mockStorageUnits() throws SQLException {
        Map<String, StorageUnit> result = new HashMap<>(2, 1F);
        result.put("ds", mockStorageUnit(spyDs));
        DataSource invalidDataSource = mock(DataSource.class);
        when(invalidDataSource.getConnection()).thenThrow(new SQLException("Mock invalid data source"));
        result.put("invalid_ds", mockStorageUnit(invalidDataSource));
        return result;
    }

    private StorageUnit mockStorageUnit(final DataSource dataSource) {
        StorageUnit result = mock(StorageUnit.class, RETURNS_DEEP_STUBS);
        when(result.getDataSource()).thenReturn(dataSource);
        return result;
    }

    private MetaDataPersistService mockMetaDataPersistService() {
        MetaDataPersistService result = mock(MetaDataPersistService.class, RETURNS_DEEP_STUBS);
        when(result.getDataSourceUnitService().load(DefaultDatabase.LOGIC_NAME))
                .thenReturn(Collections.singletonMap(DefaultDatabase.LOGIC_NAME, new DataSourcePoolProperties(HikariDataSource.class.getName(), createProperties())));
        return result;
    }

    /**
     * when call the Connection's setAutoCommit(false) method,throw an Exception
     */
    private HikariDataSource createDataSource() throws SQLException {
        HikariConfig config = new HikariConfig(createPoolProperties());
        HikariDataSource datasource = new HikariDataSource(config);
        HikariDataSource spyDataSource = spy(datasource);
        when(spyDataSource.getConnection()).then((Answer<Connection>) invocation -> {
            Connection c = datasource.getConnection();
            Connection spyConnection = spy(c);
            doAnswer(invocation1 -> {
                throw new SQLException("Test Connection leak");
            }).when(spyConnection).setAutoCommit(false);
            return spyConnection;
        });
        return spyDataSource;
    }

    private Properties createPoolProperties() {
        Properties poolProps = new Properties();
        poolProps.put("jdbcUrl", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;MODE=MySQL");
        poolProps.put("username", "root");
        poolProps.put("password", "root");
        return poolProps;
    }

    private Map<String, Object> createProperties() {
        Map<String, Object> result = new LinkedHashMap<>(3, 1F);
        result.put("jdbcUrl", "jdbc:mysql://127.0.0.1:3306/demo_ds_0?useSSL=false");
        result.put("username", "root");
        result.put("password", "123456");
        return result;
    }

    /**
     * There is  no connection leak when the setAutoCommit method do not throw SQLException
     */
    @Test
    void assertGetConnection() throws SQLException {
        int before = spyDs.getHikariPoolMXBean().getActiveConnections();
        databaseConnectionManager.setAutoCommit(true);
        for (int i = 0; i < 5; i++) {
            databaseConnectionManager.getConnections(DefaultDatabase.LOGIC_NAME, "ds", 0, 1, ConnectionMode.MEMORY_STRICTLY);
        }
        databaseConnectionManager.close();
        int after = spyDs.getHikariPoolMXBean().getActiveConnections();
        Assert.isTrue(after-before ==0);
    }


    /**
     * There is  connection leak when the setAutoCommit method throw SQLException
     */
    @Test
    void assertGetConnectionConnectionLeak() throws SQLException {
        int before = spyDs.getHikariPoolMXBean().getActiveConnections();
        databaseConnectionManager.setAutoCommit(false);
        for (int i = 0; i < 5; i++) {
            try {
                databaseConnectionManager.getConnections(DefaultDatabase.LOGIC_NAME, "ds", 0, 1, ConnectionMode.MEMORY_STRICTLY);
            } catch (SQLException e) {
                Assert.equals("Test Connection leak", e.getMessage());
            }
        }
        databaseConnectionManager.close();
        int after = spyDs.getHikariPoolMXBean().getActiveConnections();
        Assert.isTrue(after-before ==5);
    }
}