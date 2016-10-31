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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.metadata.DynamoColumnAwsMetadata;
import com.facebook.presto.dynamo.aws.metadata.DynamoTableAwsMetadata;
import com.facebook.presto.dynamo.type.DynamoType;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MockDynamoSession
        extends DynamoSession {
    static final String TEST_SCHEMA = "testkeyspace";
    static final String BAD_SCHEMA = "badkeyspace";
    static final String TEST_TABLE = "testtbl";
    static final String TEST_COLUMN1 = "column1";
    static final String TEST_COLUMN2 = "column2";
    static final String TEST_PARTITION_KEY1 = "testpartition1";
    static final String TEST_PARTITION_KEY2 = "testpartition2";

    private final AtomicInteger accessCount = new AtomicInteger();
    private boolean throwException;

    public MockDynamoSession(String connectorId, DynamoClientConfig config) {
        super(connectorId,
                "", new Identity("user", Optional.empty()),
                TimeZoneKey.UTC_KEY, Locale.ENGLISH, System.currentTimeMillis(), ImmutableMap.of(),
                null);
        this.metadata = getAwsMetadata();
    }

    public void setThrowException(boolean throwException) {
        this.throwException = throwException;
    }

    public int getAccessCount() {
        return accessCount.get();
    }

    @Override
    public AmazonDynamoDB getClient(String schemaName) {
        return new MockAmazonDynamoDB();
    }

    @Override
    public List<String> getAllSchemas() {
        accessCount.incrementAndGet();

        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_SCHEMA);
    }

    @Override
    public List<String> getAllTables(String schema)
            throws SchemaNotFoundException {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (schema.equals(TEST_SCHEMA)) {
            return ImmutableList.of(TEST_TABLE);
        }
        throw new SchemaNotFoundException(schema);
    }

    @Override
    public DynamoTable getTable(SchemaTableName tableName)
            throws TableNotFoundException {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (tableName.getSchemaName().equals(TEST_SCHEMA) && tableName.getTableName().equals(TEST_TABLE)) {
            return new DynamoTable(
                    new DynamoTableHandle(getConnectorId(), TEST_SCHEMA, TEST_TABLE),
                    ImmutableList.of(
                            new DynamoColumnHandle(getConnectorId(), TEST_COLUMN1, 0, DynamoType.STRING, null, true, false, false, false),
                            new DynamoColumnHandle(getConnectorId(), TEST_COLUMN2, 0, DynamoType.LONG, null, false, false, false, false)));
        }
        throw new TableNotFoundException(tableName);
    }

    public DynamoAwsMetadata getAwsMetadata() {
        return new DynamoAwsMetadata().setTables(getAwsTablesMetadata());
    }

    private List<DynamoTableAwsMetadata> getAwsTablesMetadata() {
        List<DynamoTableAwsMetadata> awsTabs = new ArrayList<>();
        for (String schema : getAllSchemas()) {
            for (String table : getAllTables(schema)) {

                DynamoTable tab = getTable(new SchemaTableName(schema, table));
                List<DynamoColumnAwsMetadata> cols = new ArrayList<>();
                for (DynamoColumnHandle col : tab.getColumns()) {
                    DynamoColumnAwsMetadata awsCol = new DynamoColumnAwsMetadata();
                    awsCol.setColumnName(col.getName());
                    awsCol.setColumnType(col.getDynamoType());
                    cols.add(awsCol);
                }
                DynamoTableAwsMetadata awsMeta = new DynamoTableAwsMetadata();
                awsMeta.setRegion(schema);
                awsMeta.setTableName(table);
                awsMeta.setColumns(cols);
                awsTabs.add(awsMeta);
            }
        }
        return awsTabs;
    }
}
