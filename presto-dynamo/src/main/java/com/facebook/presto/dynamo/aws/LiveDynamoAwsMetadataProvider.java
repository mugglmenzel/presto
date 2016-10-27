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
package com.facebook.presto.dynamo.aws;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.facebook.presto.dynamo.DynamoClientConfig;
import com.facebook.presto.dynamo.DynamoColumnHandle;
import com.facebook.presto.dynamo.DynamoSession;
import com.facebook.presto.dynamo.DynamoTable;
import com.facebook.presto.spi.SchemaTableName;
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
    private final DynamoClientConfig config;

    @Inject
    public LiveDynamoAwsMetadataProvider(
            DynamoSession session, DynamoClientConfig config) {
        this.session = session;
        this.config = config;
    }

    @Override
    public DynamoAwsMetadata getMetadata() {
        return new DynamoAwsMetadata().setTables(getAwsTablesMetadata());
    }

    private List<DynamoTableAwsMetadata> getAwsTablesMetadata() {
        Log.info("Retrieving all tables in all regions...");

        List<DynamoTableAwsMetadata> tablesMeta = new ArrayList<>();

        for (Region r : RegionUtils.getRegions().stream().filter(e -> !e.getName().toLowerCase().contains("gov")).collect(Collectors.toList()))
            tablesMeta.addAll(session.executeWithClient(r.getName(), new DynamoSession.ClientCallable<List<DynamoTableAwsMetadata>>() {
                @Override
                public List<DynamoTableAwsMetadata> executeWithClient(AmazonDynamoDB client) {
                    List<DynamoTableAwsMetadata> awsTabs = new ArrayList<>();
                    for (String table : client.listTables().getTableNames()) {

                        DynamoTable tab = session.getTable(new SchemaTableName(r.getName(), table));
                        List<DynamoColumnAwsMetadata> cols = new ArrayList<>();
                        for (DynamoColumnHandle col : tab.getColumns()) {
                            DynamoColumnAwsMetadata awsCol = new DynamoColumnAwsMetadata();
                            awsCol.setColumnName(col.getName());
                            awsCol.setColumnType(col.getDynamoType());
                            cols.add(awsCol);
                        }
                        DynamoTableAwsMetadata awsMeta = new DynamoTableAwsMetadata();
                        awsMeta.setRegion(r.getName());
                        awsMeta.setTableName(table);
                        awsMeta.setColumns(cols);
                        awsTabs.add(awsMeta);
                    }
                    return awsTabs;
                }
            }));

        return tablesMeta;
    }
}
