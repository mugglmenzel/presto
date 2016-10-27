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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.inject.Singleton;

import com.facebook.presto.dynamo.aws.DynamoAwsClientProvider;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadataProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

public class DynamoClientTestModule
        implements Module
{
    private final String connectorId;
    private final DynamoAwsMetadata metadata;

    public DynamoClientTestModule(String connectorId, DynamoAwsMetadata metadata)
    {
        this.connectorId = connectorId;
        this.metadata = metadata;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DynamoAwsClientProvider.class).to(MockDynamoAwsClientProvider.class);

        DynamoAwsMetadataProvider schemaProvider = new DynamoMetadataTestProvider();
        binder.bind(DynamoAwsMetadataProvider.class).toInstance(schemaProvider);

        binder.bind(DynamoConnectorId.class).toInstance(new DynamoConnectorId(connectorId));
        binder.bind(DynamoConnector.class).in(Scopes.SINGLETON);
        binder.bind(DynamoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DynamoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DynamoTokenSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DynamoRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(DynamoHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(DynamoConnectorRecordSinkProvider.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(DynamoClientConfig.class);

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
    }

    @ForDynamo
    @Singleton
    @Provides
    public static ExecutorService createCachingDynamoSchemaExecutor(DynamoConnectorId clientId, DynamoClientConfig dynamoClientConfig)
    {
        return newFixedThreadPool(
                dynamoClientConfig.getMaxSchemaRefreshThreads(),
                daemonThreadsNamed("dynamo-" + clientId + "-%s"));
    }

    @Singleton
    @Provides
    public static DynamoSession createDynamoSession(
            DynamoConnectorId connectorId,
            DynamoClientConfig config,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec)
    {
        checkNotNull(config, "config is null");
        checkNotNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        // TODO load DynamoMetadata from file
        return new DynamoSession(
                connectorId.toString(),
                "",
                new Identity("user", Optional.empty()),
                TimeZoneKey.UTC_KEY,
                Locale.ENGLISH,
                System.currentTimeMillis(), ImmutableMap.of(),
                new DynamoAwsMetadata());
    }

    public class DynamoMetadataTestProvider implements DynamoAwsMetadataProvider
    {
        @Override
        public DynamoAwsMetadata getMetadata()
        {
            return metadata;
        }
    }
}
