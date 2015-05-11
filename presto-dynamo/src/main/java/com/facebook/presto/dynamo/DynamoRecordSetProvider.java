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

import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import io.airlift.log.Logger;

import java.util.List;

import javax.inject.Inject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.facebook.presto.dynamo.aws.DynamoAwsClientProvider;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadataProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class DynamoRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(ConnectorRecordSetProvider.class);

    private final String connectorId;
    private DynamoClientConfig config;
    private final DynamoAwsClientProvider dynamoClientProvider;
    private final DynamoAwsMetadataProvider schemaProvider;

    @Inject
    public DynamoRecordSetProvider(DynamoConnectorId connectorId, DynamoClientConfig config, DynamoAwsClientProvider dynamoClientProvider, DynamoAwsMetadataProvider schemaProvider)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.config = checkNotNull(config, "config is null");
        this.dynamoClientProvider = checkNotNull(dynamoClientProvider, "dynamoClientProvider is null");
        this.schemaProvider = schemaProvider;
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        DynamoSplit dynamoSplit = checkType(split, DynamoSplit.class, "split");

        checkNotNull(columns, "columns is null");
        List<DynamoColumnHandle> dynamoColumns = ImmutableList.copyOf(transform(columns, DynamoColumnHandle.dynamoColumnHandle()));

        log.debug("Creating record set: %s", dynamoSplit.getTable());

        String schema = dynamoSplit.getSchema();
        String tableName = dynamoSplit.getTable();
        try {
            tableName = schemaProvider.getMetadata().getAwsTableName(schema, tableName);
        }
        catch (IllegalArgumentException ex) {
            log.debug(ex, "Invalid schema for AWS region: " + schema);
        }

        AmazonDynamoDB dynamoClient = dynamoClientProvider.getClient(schema);
        return new DynamoRecordSet(dynamoClient, tableName, dynamoColumns, config.getFetchSize());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }
}
