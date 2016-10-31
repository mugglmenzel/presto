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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.facebook.presto.dynamo.DynamoSession;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by menzelmi on 27/10/16.
 */
public class LiveDynamoAwsMetadataProvider implements DynamoAwsMetadataProvider {

    private Logger Log = Logger.get(LiveDynamoAwsMetadataProvider.class);

    private final DynamoSession session;

    private List<DynamoTableAwsMetadata> tablesMetadata;

    @Inject
    public LiveDynamoAwsMetadataProvider(DynamoSession session) {
        this.session = session;
    }

    @Override
    public DynamoAwsMetadata getMetadata() {
        return new DynamoAwsMetadata().setTables(getAwsTablesMetadata());
    }

    private List<DynamoTableAwsMetadata> getAwsTablesMetadata() {
        if (this.tablesMetadata == null) {
            Log.info("Retrieving all tables in all regions...");
            List<DynamoTableAwsMetadata> tempTablesMeta = new ArrayList<>();

            for (Region r : RegionUtils.getRegions().stream().filter(e -> !e.getName().toLowerCase().contains("gov")).collect(Collectors.toList()))
                tempTablesMeta.addAll(session.executeWithClient(r.getName(), new DynamoSession.ClientCallable<List<DynamoTableAwsMetadata>>() {
                    @Override
                    public List<DynamoTableAwsMetadata> executeWithClient(AmazonDynamoDB client) {
                        List<DynamoTableAwsMetadata> awsTabs = new ArrayList<>();
                        try {
                            for (String table : client.listTables().getTableNames()) {
                                Log.info("Adding table " + table + " to the metadata.");

                                List<DynamoColumnAwsMetadata> cols = new ArrayList<>();
                                for (AttributeDefinition col : client.describeTable(table).getTable().getAttributeDefinitions()) {
                                    Log.info("Adding column " + col + " to the metadata.");
                                    DynamoColumnAwsMetadata awsCol = new DynamoColumnAwsMetadata();
                                    awsCol.setColumnName(col.getAttributeName().toLowerCase());
                                    awsCol.setColumnType(AwsDynamoToPrestoTypeMapper.map(col.getAttributeType()));
                                    cols.add(awsCol);
                                }
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
            Log.info("Generated new AWS Metadata: " + tempTablesMeta);
            this.tablesMetadata = tempTablesMeta;
        }

        return tablesMetadata;
    }
}
