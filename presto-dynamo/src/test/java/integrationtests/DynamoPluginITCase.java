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
package integrationtests;

import com.facebook.presto.dynamo.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class DynamoPluginITCase {
    private static final Logger log = Logger.get(DynamoPluginITCase.class);

    private final ConnectorSession SESSION = new TestingConnectorSession(
            "user", UTC_KEY, ENGLISH, System.currentTimeMillis(), ImmutableList.of(), ImmutableMap.of());
    private final DynamoTransactionHandle TX = DynamoTransactionHandle.INSTANCE;

    @Test
    public void testMetadata() throws Exception {
        String metadataFilePath = DynamoTestingUtils.createTestMetadataFile();

        Map<String, String> requiredConfig = new HashMap<String, String>();
        requiredConfig.put("dynamo.metadata-file", metadataFilePath);

        DynamoPlugin plugin = new DynamoPlugin();

        List<ConnectorFactory> services = Lists.newArrayList(plugin
                .getConnectorFactories());
        Assert.assertEquals(services.size(), 1);

        ConnectorFactory factory = services.get(0);
        Assert.assertEquals(factory.getClass(), DynamoConnectorFactory.class);

        ConnectorSession session = new TestingConnectorSession("user",
                TimeZoneKey.UTC_KEY, Locale.ENGLISH,
                System.currentTimeMillis(), null, null);
        Connector connector = factory.create(
                "dynamo-integration-test-connector", requiredConfig, new TestingConnectorContext());
        ConnectorMetadata connectorMetadata = connector.getMetadata(TX);

        List<String> schemaNames = connectorMetadata.listSchemaNames(
                session);
        Collections.sort(schemaNames);
        Assert.assertEquals(schemaNames, ImmutableList.of("us_west_1", "us_west_2"));

        List<SchemaTableName> schemaTableNames = connectorMetadata
                .listTables(null, null);
        Assert.assertEquals(schemaTableNames.size(), 3);

        schemaTableNames = connectorMetadata
                .listTables(null, "us_west_1");
        Assert.assertEquals(schemaTableNames.size(), 2);

        schemaTableNames = connectorMetadata
                .listTables(null, "us_west_2");
        Assert.assertEquals(schemaTableNames.size(), 1);

        schemaTableNames = connectorMetadata
                .listTables(null, "us_east_1");
        Assert.assertEquals(schemaTableNames.size(), 0);

        ConnectorTableHandle tableHandle = connectorMetadata
                .getTableHandle(session,
                        new SchemaTableName("us_west_2", "Users"));

        List<ColumnHandle> columnHandles = ImmutableList
                .copyOf(connectorMetadata.getColumnHandles(SESSION, tableHandle)
                        .values());

        ConnectorSplitManager splitManager = connector.getSplitManager();
        ConnectorSplitSource splitSource = splitManager.getSplits(TX, SESSION, new DynamoTableLayoutHandle((DynamoTableHandle) tableHandle));
        List<ConnectorSplit> splits = new ArrayList<ConnectorSplit>();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000).get();
            splits.addAll(batch);
        }

        for (ConnectorSplit split : splits) {
            RecordSet rs = connector.getRecordSetProvider().getRecordSet(TX, SESSION, split,
                    columnHandles);
            try (RecordCursor cursor = rs.cursor()) {
                int rowIndex = 0;
                while (cursor.advanceNextPosition()) {
                    log.info(String.format("---------- Row %s ----------",
                            rowIndex++));
                    {
                        int columnIndex = 0;
                        DynamoColumnHandle columnHandle = (DynamoColumnHandle) columnHandles
                                .get(columnIndex);
                        Type type = cursor.getType(columnIndex);
                        Slice slice = cursor.getSlice(columnIndex);
                        String strValue = slice.toStringUtf8();
                        log.info(String.format("Column: %s, %s, %s",
                                columnHandle.getName(), type, strValue));
                    }
                    {
                        int columnIndex = 1;
                        DynamoColumnHandle columnHandle = (DynamoColumnHandle) columnHandles
                                .get(columnIndex);
                        Type type = cursor.getType(columnIndex);
                        boolean isNull = cursor.isNull(columnIndex);
                        if (isNull) {
                            log.info(String.format("Column: %s, %s, %s",
                                    columnHandle.getName(), type, "[NULL]"));
                        } else {
                            long value = cursor.getLong(columnIndex);
                            log.info(String.format("Column: %s, %s, %s",
                                    columnHandle.getName(), type, value));
                        }
                    }
                }
            }
        }
    }
}
