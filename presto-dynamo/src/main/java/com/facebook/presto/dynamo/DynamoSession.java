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

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.facebook.presto.dynamo.aws.AwsUtils;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoColumnAwsMetadata;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class DynamoSession
{
    static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";
    protected final String connectorId;

    private DynamoAwsMetadata metadata;

    private LoadingCache<String, AmazonDynamoDB> clientBySchema;

    public DynamoSession(String connectorId, DynamoAwsMetadata metadata)
    {
        this.connectorId = connectorId;

        this.metadata = metadata == null ? new DynamoAwsMetadata() : metadata;

        clientBySchema = CacheBuilder.newBuilder().build(
                new CacheLoader<String, AmazonDynamoDB>()
                {
                    @Override
                    public AmazonDynamoDB load(String key) throws Exception
                    {
                        AmazonDynamoDBClient client = new AmazonDynamoDBClient(
                                new DefaultAWSCredentialsProviderChain()
                                        .getCredentials());
                        client.setRegion(RegionUtils.getRegion(AwsUtils.getRegionByEnumName(key).getName()));
                        // client.setRegion(Region.getRegion(Regions.US_WEST_2));
                        return client;
                    }
                });
    }

    private AmazonDynamoDB getClient(String schemaName)
    {
        try {
            return clientBySchema.get(schemaName);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public List<String> getAllSchemas()
    {
        return this.metadata.getRegionsAsSchemaNames();
    }

    public List<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        return executeWithClient(schema, new ClientCallable<List<String>>()
        {
            @Override
            public List<String> executeWithClient(AmazonDynamoDB client)
            {
                ListTablesResult tables = client.listTables();
                return tables.getTableNames();
            }
        });
    }

    public DynamoTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        return DynamoTable.getTable(metadata, connectorId,
                tableName.getSchemaName(), tableName.getTableName());
    }

    private DynamoColumnHandle buildColumnHandle(
            DynamoColumnAwsMetadata columnMeta, boolean partitionKey,
            boolean clusteringKey, int ordinalPosition, boolean hidden)
    {
        DynamoType dynamoType = columnMeta.getColumnType();
        List<DynamoType> typeArguments = null;
        if (dynamoType != null && dynamoType.getTypeArgumentSize() > 0) {
            List<DynamoType> typeArgs = columnMeta.getTypeArguments();
            switch (dynamoType.getTypeArgumentSize()) {
            case 1:
                typeArguments = ImmutableList.of(typeArgs.get(0));
                break;
            case 2:
                typeArguments = ImmutableList.of(typeArgs.get(0),
                        typeArgs.get(1));
                break;
            default:
                throw new IllegalArgumentException("Invalid type arguments: "
                        + typeArgs);
            }
        }
        return new DynamoColumnHandle(connectorId, columnMeta.getColumnName(),
                ordinalPosition, dynamoType, typeArguments, partitionKey,
                clusteringKey, false, hidden);
    }

    public <T> T executeWithClient(String schemaName,
            ClientCallable<T> clientCallable)
    {
        Exception lastException = null;
        for (int i = 0; i < 2; i++) {
            AmazonDynamoDB client = getClient(schemaName);
            try {
                return clientCallable.executeWithClient(client);
            }
            catch (Exception e) {
                lastException = e;

                // Something happened with our client connection. We need to
                // re-establish the connection using our contact points.
                clientBySchema.asMap().remove(schemaName, client);
            }
        }
        throw new RuntimeException(lastException);
    }

    private interface ClientCallable<T>
    {
        T executeWithClient(AmazonDynamoDB client);
    }
}
