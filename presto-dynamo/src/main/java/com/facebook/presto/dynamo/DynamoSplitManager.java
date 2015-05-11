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
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import com.facebook.presto.dynamo.aws.DynamoAwsMetadataProvider;
import com.facebook.presto.dynamo.util.HostAddressFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;

public class DynamoSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(DynamoSplitManager.class);

    private final String connectorId;
    private final DynamoSession dynamoSession;
    private final DynamoAwsMetadataProvider schemaProvider;
    private final int partitionSizeForBatchSelect;
    private final DynamoTokenSplitManager tokenSplitMgr;
    private final ListeningExecutorService executor;

    @Inject
    public DynamoSplitManager(DynamoConnectorId connectorId,
            DynamoClientConfig dynamoClientConfig,
            DynamoSession dynamoSession,
            DynamoAwsMetadataProvider schemaProvider,
            DynamoTokenSplitManager tokenSplitMgr,
            @ForDynamo ExecutorService executor)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.dynamoSession = checkNotNull(dynamoSession, "dynamoSession is null");
        this.partitionSizeForBatchSelect = dynamoClientConfig.getPartitionSizeForBatchSelect();
        this.tokenSplitMgr = tokenSplitMgr;
        this.executor = listeningDecorator(executor);
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        DynamoTableHandle dynamoTableHandle = checkType(tableHandle, DynamoTableHandle.class, "tableHandle");
        checkNotNull(tupleDomain, "tupleDomain is null");
        DynamoTable table = DynamoTable.getTable(schemaProvider.getMetadata(), connectorId, dynamoTableHandle.getSchemaName(), dynamoTableHandle.getTableName());
        List<DynamoColumnHandle> partitionKeys = table.getPartitionKeyColumns();

        // fetch the partitions
        List<DynamoPartition> allPartitions = getDynamoPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", dynamoTableHandle.getSchemaName(), dynamoTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<ConnectorPartition> partitions = FluentIterable.from(allPartitions)
                .filter(partitionMatches(tupleDomain))
                .filter(ConnectorPartition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && ((DynamoPartition) partitions.get(0)).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<ColumnHandle> partitionColumns = (List) partitionKeys;
                remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionColumns))));
            }
        }

        // push down indexed column fixed value predicates only for unpartitioned partition which uses token range query
        if (partitions.size() == 1 && ((DynamoPartition) partitions.get(0)).isUnpartitioned()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains();
            List<ColumnHandle> indexedColumns = Lists.newArrayList();
            // TODO compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            if (sb.length() > 0) {
                DynamoPartition partition = (DynamoPartition) partitions.get(0);
                TupleDomain<ColumnHandle> filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains(), not(in(indexedColumns))));
                partitions = Lists.newArrayList();
                partitions.add(new DynamoPartition(partition.getKey(), sb.toString(), filterIndexedColumn, true));
                return new ConnectorPartitionResult(partitions, filterIndexedColumn);
            }
        }
        return new ConnectorPartitionResult(partitions, remainingTupleDomain);
    }

    private List<DynamoPartition> getDynamoPartitions(final DynamoTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of();
        }

        return ImmutableList.of(DynamoPartition.UNPARTITIONED);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        DynamoTableHandle dynamoTableHandle = checkType(tableHandle, DynamoTableHandle.class, "tableHandle");

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            ConnectorPartition partition = partitions.get(0);
            DynamoPartition dynamoPartition = checkType(partition, DynamoPartition.class, "partition");

            if (dynamoPartition.isUnpartitioned() || dynamoPartition.isIndexedColumnPredicatePushdown()) {
                DynamoTable table = DynamoTable.getTable(schemaProvider.getMetadata(), connectorId, dynamoTableHandle.getSchemaName(), dynamoTableHandle.getTableName());
                List<ConnectorSplit> splits = getSplitsByTokenRange(table, dynamoPartition.getPartitionId());
                return new FixedSplitSource(connectorId, splits);
            }
        }

        throw new RuntimeException("Partition not supported for Dynamo yet");
    }

    private List<ConnectorSplit> getSplitsByTokenRange(DynamoTable table, String partitionId)
    {
        String schema = table.getTableHandle().getSchemaName();
        String tableName = table.getTableHandle().getTableName();
        String tokenExpression = table.getTokenExpression();

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        List<DynamoTokenSplitManager.TokenSplit> tokenSplits;
        try {
            tokenSplits = tokenSplitMgr.getSplits(schema, tableName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (DynamoTokenSplitManager.TokenSplit tokenSplit : tokenSplits) {
            String condition = buildTokenCondition(tokenExpression, tokenSplit.getStartToken(), tokenSplit.getEndToken());
            List<HostAddress> addresses = new HostAddressFactory().AddressNamesToHostAddressList(tokenSplit.getHosts());
            DynamoSplit split = new DynamoSplit(connectorId, schema, tableName, partitionId, condition, addresses);
            builder.add(split);
        }

        return builder.build();
    }

    private static String buildTokenCondition(String tokenExpression, String startToken, String endToken)
    {
        return tokenExpression + " > " + startToken + " AND " + tokenExpression + " <= " + endToken;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    public static Predicate<DynamoPartition> partitionMatches(final TupleDomain<ColumnHandle> tupleDomain)
    {
        return new Predicate<DynamoPartition>()
        {
            @Override
            public boolean apply(DynamoPartition partition)
            {
                return tupleDomain.overlaps(partition.getTupleDomain());
            }
        };
    }
}
