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
package com.facebook.presto.dynamo.aws.metadata;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.facebook.presto.dynamo.DynamoClientConfig;
import com.facebook.presto.dynamo.DynamoSession;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by menzelmi on 27/10/16.
 */
public class LiveDynamoAwsMetadataProvider implements DynamoAwsMetadataProvider {

    private Logger Log = Logger.get(LiveDynamoAwsMetadataProvider.class);

    private final DynamoSession session;

    private final DynamoClientConfig config;

    private List<DynamoTableAwsMetadata> tablesMetadataCache;

    private long lastSchemaRefresh;

    @Inject
    public LiveDynamoAwsMetadataProvider(DynamoSession session, DynamoClientConfig config) {
        this.session = session;
        this.config = config;
        this.tablesMetadataCache = generateAwsTablesMetadata();
        this.lastSchemaRefresh = System.currentTimeMillis();
    }

    @Override
    public DynamoAwsMetadata getMetadata() {
        return new DynamoAwsMetadata().setTables(getAwsTablesMetadata());
    }

    private List<DynamoTableAwsMetadata> getAwsTablesMetadata() {
        if (this.tablesMetadataCache == null || this.lastSchemaRefresh + config.getSchemaCacheTtl().toMillis() < System.currentTimeMillis()) {
            this.tablesMetadataCache = generateAwsTablesMetadata();
            this.lastSchemaRefresh = System.currentTimeMillis();
        }

        return tablesMetadataCache;
    }

    private List<DynamoTableAwsMetadata> generateAwsTablesMetadata() {
        Log.info("Retrieving all tables in all regions...");
        List<DynamoTableAwsMetadata> tempTablesMeta = Collections.synchronizedList(new ArrayList<>());

        RegionUtils.getRegions().parallelStream().filter(e -> !e.getName().toLowerCase().contains("gov")).forEach(r -> {
            tempTablesMeta.addAll(session.executeWithClient(r.getName(), new DynamoSession.ClientCallable<List<DynamoTableAwsMetadata>>() {
                @Override
                public List<DynamoTableAwsMetadata> executeWithClient(AmazonDynamoDB client) {
                    List<DynamoTableAwsMetadata> awsTabs = new ArrayList<>();
                    try {
                        for (String table : client.listTables().getTableNames()) {
                            Log.info("Adding table " + table + " to the metadata.");

                            List<DynamoColumnAwsMetadata> cols = new ArrayList<>();
                            client.scan(new ScanRequest().withLimit(1).withTableName(table)).getItems().stream().findFirst().map(m -> m.keySet().stream().map(c -> new AttributeDefinition().withAttributeName(c).withAttributeType(ScalarAttributeType.S)))
                                    .orElse(client.describeTable(table).getTable().getAttributeDefinitions().stream()).forEach(col -> {
                                Log.info("Adding column " + col + " to the metadata.");
                                DynamoColumnAwsMetadata awsCol = new DynamoColumnAwsMetadata();
                                awsCol.setColumnName(col.getAttributeName().toLowerCase());
                                awsCol.setColumnType(AwsDynamoToPrestoTypeMapper.map(col.getAttributeType()));
                                cols.add(awsCol);
                            });
                            DynamoTableAwsMetadata awsMeta = new DynamoTableAwsMetadata();
                            awsMeta.setRegion(r.getName());
                            awsMeta.setTableName(table);
                            awsMeta.setColumns(cols);
                            awsTabs.add(awsMeta);
                        }
                    } catch (AmazonDynamoDBException e) {
                        Log.warn("Could not fetch tables for region " + r.getName() + ".");
                    }
                    return awsTabs;
                }
            }));
        });

        Log.info("Generated new AWS Metadata: " + tempTablesMeta);
        return tempTablesMeta;
    }
}
