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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.metadata.DynamoColumnAwsMetadata;
import com.facebook.presto.dynamo.type.DynamoType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class DynamoSession implements ConnectorSession {

    private final Logger Log = Logger.get(DynamoSession.class);

    private final String connectorId;
    private final String queryId;
    private final Identity identity;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final long startTime;
    private final Map<String, String> properties = new HashMap<>();


    protected DynamoAwsMetadata metadata;

    private LoadingCache<String, AmazonDynamoDB> clientBySchema;

    public DynamoSession(String connectorId, String queryId, Identity identity, TimeZoneKey timeZoneKey, Locale locale, long startTime, Map<String, String> properties, DynamoAwsMetadata metadata) {
        this.connectorId = connectorId;
        this.queryId = queryId;
        this.identity = identity;
        this.timeZoneKey = timeZoneKey;
        this.locale = locale;
        this.startTime = startTime;
        if (properties != null) this.properties.putAll(properties);
        this.metadata = metadata == null ? new DynamoAwsMetadata() : metadata;

        clientBySchema = CacheBuilder.newBuilder().build(
                new CacheLoader<String, AmazonDynamoDB>() {
                    @Override
                    public AmazonDynamoDB load(String region) throws Exception {
                        return new AmazonDynamoDBClient(
                                new DefaultAWSCredentialsProviderChain()
                                        .getCredentials()).withRegion(Regions.fromName(region));
                    }
                });
        Log.debug("New DynamoSession created.");
    }

    public static DynamoSession fromConnectorSession(ConnectorSession session) {
        return new DynamoSession("", session.getQueryId(), session.getIdentity(), session.getTimeZoneKey(), session.getLocale(), session.getStartTime(), Collections.emptyMap(), new DynamoAwsMetadata());
    }

    public AmazonDynamoDB getClient(String schemaName) {
        try {
            Log.debug("Getting client for " + schemaName);
            return clientBySchema.get(schemaName);
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public List<String> getAllSchemas() {
        return getMetadata().getRegionsAsSchemaNames();
    }

    public List<String> getAllTables(String schema)
            throws SchemaNotFoundException {
        Log.info("Retrieving all tables for " + schema);
        return getMetadata().getTableNames(schema);
    }

    public DynamoTable getTable(SchemaTableName tableName)
            throws TableNotFoundException {
        return DynamoTable.getTable(getMetadata(), getConnectorId(),
                tableName.getSchemaName(), tableName.getTableName());
    }

    private DynamoColumnHandle buildColumnHandle(
            DynamoColumnAwsMetadata columnMeta, boolean partitionKey,
            boolean clusteringKey, int ordinalPosition, boolean hidden) {
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
                                   ClientCallable<T> clientCallable) {
        Exception lastException = null;
        for (int i = 0; i < 2; i++) {
            AmazonDynamoDB client = getClient(schemaName);
            try {
                return clientCallable.executeWithClient(client);
            } catch (Exception e) {
                lastException = e;

                // Something happened with our client connection. We need to
                // re-establish the connection using our contact points.
                clientBySchema.asMap().remove(schemaName, client);
            }
        }
        throw new RuntimeException(lastException);
    }

    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public String getQueryId() {
        return queryId;
    }

    @Override
    public Identity getIdentity() {
        return identity;
    }

    @Override
    public TimeZoneKey getTimeZoneKey() {
        return timeZoneKey;
    }

    @Override
    public Locale getLocale() {
        return locale;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    @Override
    public <T> T getProperty(String name, Class<T> type) {
        return type.cast(properties.get(name));
    }

    public DynamoAwsMetadata getMetadata() {
        return metadata;
    }

    public interface ClientCallable<T> {
        T executeWithClient(AmazonDynamoDB client);
    }

    @Override
    public String toString() {
        return "DynamoSession{" +
                "connectorId='" + connectorId + '\'' +
                ", queryId='" + queryId + '\'' +
                ", identity=" + identity +
                ", timeZoneKey=" + timeZoneKey +
                ", locale=" + locale +
                ", startTime=" + startTime +
                ", properties=" + properties +
                ", metadata=" + metadata +
                '}';
    }
}
