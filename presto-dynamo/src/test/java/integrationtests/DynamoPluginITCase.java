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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazonaws.regions.Regions;
import com.facebook.presto.dynamo.DynamoColumnHandle;
import com.facebook.presto.dynamo.DynamoConnectorFactory;
import com.facebook.presto.dynamo.DynamoPlugin;
import com.facebook.presto.dynamo.DynamoType;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoColumnAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoTableAwsMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class DynamoPluginITCase
{
    private static final Logger log = Logger.get(DynamoPluginITCase.class);

    @Test
    public void testMetadata() throws Exception
    {
        File tempFile = File.createTempFile("dynamo-metadata-test", ".tmp");
        tempFile.deleteOnExit();

        String metadataFilePath = tempFile.getAbsolutePath();

        DynamoAwsMetadata metadata = new DynamoAwsMetadata();
        {
            List<DynamoColumnAwsMetadata> columns = new ArrayList<DynamoColumnAwsMetadata>();
            columns.add(new DynamoColumnAwsMetadata("UserId",
                    DynamoType.STRING, null));
            columns.add(new DynamoColumnAwsMetadata("Version",
                    DynamoType.LONG, null));
            DynamoTableAwsMetadata table = new DynamoTableAwsMetadata(
                    Regions.US_WEST_2.toString().toLowerCase(), "Users", columns);
            metadata.getTables().add(table);
        }
        {
            List<DynamoColumnAwsMetadata> columns = new ArrayList<DynamoColumnAwsMetadata>();
            columns.add(new DynamoColumnAwsMetadata("BookName",
                    DynamoType.STRING, null));
            columns.add(new DynamoColumnAwsMetadata("Writers", DynamoType.LIST,
                    ImmutableList.of(DynamoType.STRING)));
            DynamoTableAwsMetadata table = new DynamoTableAwsMetadata(
                    Regions.US_WEST_2.toString().toLowerCase(), "Books", columns);
            metadata.getTables().add(table);
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(tempFile, metadata);

        String json = mapper.writeValueAsString(metadata);
        log.info("AWS Metadata: " + json);

        Map<String, String> requiredConfig = new HashMap<String, String>();
        requiredConfig.put("dynamo.metadata-file", metadataFilePath);

        Map<String, String> optionalConfig = new HashMap<String, String>();

        DynamoPlugin plugin = new DynamoPlugin();
        plugin.setOptionalConfig(optionalConfig);

        List<ConnectorFactory> services = plugin
                .getServices(ConnectorFactory.class);
        Assert.assertEquals(services.size(), 1);

        ConnectorFactory factory = services.get(0);
        Assert.assertEquals(factory.getClass(), DynamoConnectorFactory.class);

        ConnectorSession session = new ConnectorSession("user",
                TimeZoneKey.UTC_KEY, Locale.ENGLISH,
                System.currentTimeMillis(), null);
        Connector connector = factory.create(
                "dynamo-integration-test-connector", requiredConfig);

        List<String> schemaNames = connector.getMetadata().listSchemaNames(
                session);
        Assert.assertEquals(schemaNames, metadata.getRegionsAsSchemaNames());

        List<SchemaTableName> schemaTableNames = connector.getMetadata()
                .listTables(null, null);
        Assert.assertEquals(schemaTableNames.size(), 2);

        schemaTableNames = connector.getMetadata()
                .listTables(null, "us_west_2");
        Assert.assertEquals(schemaTableNames.size(), 2);

        schemaTableNames = connector.getMetadata()
                .listTables(null, "us_west_1");
        Assert.assertEquals(schemaTableNames.size(), 0);

        ConnectorMetadata connectorMetadata = connector.getMetadata();
        ConnectorTableHandle tableHandle = connector.getMetadata()
                .getTableHandle(session,
                        new SchemaTableName("us_west_2", "Users"));

        List<ConnectorColumnHandle> columnHandles = ImmutableList
                .copyOf(connectorMetadata.getColumnHandles(tableHandle)
                        .values());

        ConnectorSplitManager splitManager = connector.getSplitManager();
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(
                tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(
                tableHandle, partitionResult.getPartitions());
        List<ConnectorSplit> splits = new ArrayList<ConnectorSplit>();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000);
            splits.addAll(batch);
        }

        for (ConnectorSplit split : splits) {
            RecordSet rs = connector.getRecordSetProvider().getRecordSet(split,
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
                        }
                        else {
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
