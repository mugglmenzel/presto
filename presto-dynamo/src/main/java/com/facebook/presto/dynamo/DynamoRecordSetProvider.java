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
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadataProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class DynamoRecordSetProvider
        implements ConnectorRecordSetProvider {
    private static final Logger Log = Logger.get(ConnectorRecordSetProvider.class);

    private final String connectorId;
    private DynamoClientConfig config;
    private final DynamoAwsMetadataProvider schemaProvider;

    @Inject
    public DynamoRecordSetProvider(DynamoConnectorId connectorId, DynamoClientConfig config, DynamoAwsMetadataProvider schemaProvider) {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.config = checkNotNull(config, "config is null");
        this.schemaProvider = schemaProvider;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        session = DynamoSession.fromConnectorSession(session);
        Log.info("Session for records is: " + session);
        DynamoSession dynamoSession = checkType(session, DynamoSession.class, "session");
        DynamoSplit dynamoSplit = checkType(split, DynamoSplit.class, "split");

        checkNotNull(columns, "columns is null");
        List<DynamoColumnHandle> dynamoColumns = ImmutableList.copyOf(transform(columns, DynamoColumnHandle.dynamoColumnHandle()));

        Log.info("Creating record set: %s", dynamoSplit.getTable());

        String schema = dynamoSplit.getSchema();
        String tableName = dynamoSplit.getTable();
        try {
            tableName = schemaProvider.getMetadata().getAwsTableName(schema, tableName);
        } catch (IllegalArgumentException ex) {
            Log.warn(ex, "Invalid schema for AWS region: " + schema);
        }

        AmazonDynamoDB dynamoClient = dynamoSession.getClient(schema);
        return new DynamoRecordSet(dynamoClient, tableName, dynamoSplit.getPartitionId(), dynamoSplit.getPartitionCount(), dynamoColumns, config.getFetchSize());
    }
}
