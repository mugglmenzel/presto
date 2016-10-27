/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.dynamo;

import com.amazonaws.regions.Regions;
import com.facebook.presto.dynamo.aws.AwsUtils;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoColumnAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoTableAwsMetadata;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.slice.Slice;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.*;

import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.*;

@Test(singleThreaded = true)
public class TestDynamoConnector {
    private static final String connectorId = "dynamo-test";
    private static final MockDynamoSession SESSION = new MockDynamoSession(connectorId, null);
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;
    private final ConnectorTransactionHandle TX = DynamoTransactionHandle.INSTANCE;

    @BeforeClass
    public void setup() throws Exception {


        Connector connector = createConnector(connectorId);

        assertInstanceOf(TX, DynamoTransactionHandle.class);

        metadata = connector.getMetadata(TX);
        assertInstanceOf(metadata, DynamoMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, DynamoSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, DynamoRecordSetProvider.class);

        database = MockDynamoSession.TEST_SCHEMA.toLowerCase();
        table = new SchemaTableName(database, MockDynamoSession.TEST_TABLE.toLowerCase());
        tableUnpartitioned = new SchemaTableName(database,
                "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database,
                "totally_invalid_table_name");
    }

    @AfterMethod
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetClient() {
    }

    @Test
    public void testGetDatabaseNames() throws Exception {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertTrue(schemas.contains(AwsUtils.getRegionAsSchemaName(Regions.US_WEST_2.toString().toLowerCase())));
    }

    @Test
    public void testGetTableNames() throws Exception {
        List<SchemaTableName> tables = metadata.listTables(SESSION, database);
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and
    // schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException() throws Exception {
        metadata.listTables(SESSION, INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema() {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName(
                "totally_invalid_database_name", "dual")));
        assertEquals(
                metadata.listTables(SESSION, "totally_invalid_database_name"),
                ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix(
                "totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetRecords() throws Exception {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata
                .getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList
                .copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);


        ConnectorSplitSource splitSource = splitManager.getSplits(TX, SESSION,
                new DynamoTableLayoutHandle((DynamoTableHandle) tableHandle));

        List<ConnectorSplit> splits = new ArrayList<ConnectorSplit>();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(10000).get();
            splits.addAll(batch);
        }

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            DynamoSplit dynamoSplit = (DynamoSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(
                    TX, SESSION, dynamoSplit, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    } catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    int idx = columnIndex.get(MockDynamoSession.TEST_COLUMN1);
                    Slice slice = cursor
                            .getSlice(idx);
                    String keyValue = slice.toStringUtf8();
                    assertTrue(keyValue != null);

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertTrue(rowNumber > 0);
    }

    private static void assertReadFields(RecordCursor cursor,
                                         List<ColumnMetadata> schema) {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                Type type = column.getType();
                if (BOOLEAN.equals(type)) {
                    cursor.getBoolean(columnIndex);
                } else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                } else if (TIMESTAMP.equals(type)) {
                    cursor.getLong(columnIndex);
                } else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                } else if (VARCHAR.equals(type)) {
                    try {
                        cursor.getSlice(columnIndex);
                    } catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                } else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName) {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION,
                tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static ImmutableMap<String, Integer> indexColumns(
            List<ColumnHandle> columnHandles) {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = checkType(columnHandle, DynamoColumnHandle.class,
                    "columnHandle").getName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }

    private static Connector createConnector(String connectorId) {
        Map<String, String> config = new HashMap<String, String>();
        DynamoAwsMetadata metadata = SESSION.getAwsMetadata();
        List<DynamoColumnAwsMetadata> columns = new ArrayList<DynamoColumnAwsMetadata>();
        columns.add(new DynamoColumnAwsMetadata("UserId", DynamoType.STRING, null));
        DynamoTableAwsMetadata table = new DynamoTableAwsMetadata(
                Regions.US_WEST_2.toString().toLowerCase(), MockDynamoSession.TEST_TABLE,
                columns);
        metadata.getTables().add(table);
        try {
            Bootstrap app = new Bootstrap(new MBeanModule(), new JsonModule(),
                    new DynamoClientTestModule(connectorId, metadata),
                    new Module() {
                        @Override
                        public void configure(Binder binder) {
                            MBeanServer platformMBeanServer = ManagementFactory
                                    .getPlatformMBeanServer();
                            binder.bind(MBeanServer.class).toInstance(
                                    new RebindSafeMBeanServer(
                                            platformMBeanServer));
                        }
                    });

            Injector injector = app.strictConfig().doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config).initialize();

            return injector.getInstance(DynamoConnector.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
