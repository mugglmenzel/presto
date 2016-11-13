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
import com.amazonaws.services.dynamodbv2.model.*;
import com.facebook.presto.dynamo.DynamoClientConfig;
import com.facebook.presto.dynamo.DynamoSession;
import io.airlift.log.Logger;
import org.joda.time.format.DateTimeFormat;

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
                        ListTablesResult tables;
                        String lastKey = null;
                        do {
                            tables = client.listTables(lastKey);
                            lastKey = tables.getLastEvaluatedTableName();
                            for (String table : tables.getTableNames()) {
                                Log.info(String.format("Adding table %s to the metadata.", table));

                                List<DynamoColumnAwsMetadata> cols = new ArrayList<>();
                                client.scan(new ScanRequest().withLimit(1).withTableName(table)).getItems().stream().findFirst().map(m ->
                                        m.entrySet().stream().map(c ->
                                                new AttributeDefinition().withAttributeName(c.getKey()).withAttributeType(detectType(c.getValue()))
                                        )
                                ).orElse(client.describeTable(table).getTable().getAttributeDefinitions().stream()).forEach(col -> {
                                    Log.info(String.format("Adding column %s to the metadata.", col));
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

                        } while (!(lastKey == null || "".equals(lastKey)));
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

    private String detectType(AttributeValue av) {
        if(av == null) return "S";
        if (av.getBOOL() != null) return "BOOL";
        if (av.getB() != null) return "B";
        if (av.getNULL() != null && av.getNULL()) return "NULL";
        if (av.getN() != null) {
            try {
                Integer.parseInt(av.getN());
                return "INT";
            } catch (NumberFormatException e) {
                Log.debug(String.format("Could not parse %s as Double.", av.getN()));
            }
            try {
                Long.parseLong(av.getN());
                return "LONG";
            } catch (NumberFormatException e) {
                Log.debug(String.format("Could not parse %s as Double.", av.getN()));
            }
            return "N";
        }
        if (av.getS() != null) {
            try {
                DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(av.getS());
                return "DATE";
            } catch (IllegalArgumentException e) {
                Log.debug(String.format("Could not parse %s as Date. Message: %s", av.getS(), e.getMessage()));
            }
            try {
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(av.getS());
                return "DATETIME";
            } catch (IllegalArgumentException e) {
                Log.debug(String.format("Could not parse %s as DateTime. Message: %s", av.getS(), e.getMessage()));
            }
        }
        return "S";
    }
}
