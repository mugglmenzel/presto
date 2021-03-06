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

import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadataProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

public class DynamoSplitManager
        implements ConnectorSplitManager {
    private static final Logger Log = Logger.get(DynamoSplitManager.class);

    private final String connectorId;
    private final DynamoSession dynamoSession;
    private final DynamoAwsMetadataProvider schemaProvider;
    private final DynamoClientConfig clientConfig;
    private final ListeningExecutorService executor;

    @Inject
    public DynamoSplitManager(DynamoConnectorId connectorId,
                              DynamoClientConfig dynamoClientConfig,
                              DynamoSession dynamoSession,
                              DynamoAwsMetadataProvider schemaProvider,
                              @ForDynamo ExecutorService executor) {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.dynamoSession = checkNotNull(dynamoSession, "dynamoSession is null");
        this.clientConfig = dynamoClientConfig;
        this.executor = listeningDecorator(executor);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout) {
        DynamoTableLayoutHandle dynamoTableLayoutHandle = checkType(layout, DynamoTableLayoutHandle.class, "layout");

        Log.debug(String.format("Getting splits in schema provider %s ...", schemaProvider));

        return new FixedSplitSource(getSplitsList(dynamoTableLayoutHandle));
    }


    public List<DynamoSplit> getSplitsList(DynamoTableLayoutHandle dynamoTableLayoutHandle) {
        return getSplitsList(dynamoTableLayoutHandle.getTable());
    }

    public List<DynamoSplit> getSplitsList(DynamoTableHandle dynamoTableHandle) {
        List<DynamoSplit> splits = new ArrayList<>();
        Integer totalPartitions = numberOfSplits(dynamoTableHandle);
        for (int i = 0; i < totalPartitions; i++)
            splits.add(new DynamoSplit(connectorId, dynamoTableHandle.getSchemaName(), dynamoTableHandle.getTableName(), i, totalPartitions, "", ImmutableList.of()));
        return splits;
    }

    private Integer numberOfSplits(DynamoTableHandle dynamoTableHandle) {
        Log.debug(String.format("Looking for table %s %s", dynamoTableHandle.getSchemaName(), dynamoTableHandle.getTableName()));
        String tableName = schemaProvider.getMetadata().getAwsTableName(dynamoTableHandle.getSchemaName(), dynamoTableHandle.getTableName());
        Log.debug(String.format("Found actual AWS table name %s", tableName));
        Long itemCount = dynamoSession
                .getClient(dynamoTableHandle.getSchemaName())
                .describeTable(tableName)
                .getTable()
                .getItemCount();

        Log.debug("Returning splits: " + itemCount + "/" + clientConfig.getSplitSize());
        return Math.max(itemCount.intValue() / clientConfig.getSplitSize(), clientConfig.getMinSplitCount());
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

}
