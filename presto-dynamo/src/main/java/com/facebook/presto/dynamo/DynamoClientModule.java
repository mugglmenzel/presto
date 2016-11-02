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

import com.facebook.presto.dynamo.aws.*;
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadataProvider;
import com.facebook.presto.dynamo.aws.metadata.LiveDynamoAwsMetadataProvider;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;

import javax.inject.Singleton;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DynamoClientModule
        implements Module {
    private final String connectorId;

    public DynamoClientModule(String connectorId) {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(DynamoClientConfig.class);

        binder.bind(DynamoAwsClientProvider.class).to(DefaultDynamoAwsClientProvider.class).in(Scopes.SINGLETON);

        //ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        //CachingDynamoMetadataProvider schemaProvider = new CachingDynamoMetadataProvider(
        //        executor,
        //        new Duration(5, TimeUnit.MINUTES),
        //        new Duration(1, TimeUnit.MINUTES),
        //        "/tmp/dynamo-metadata.json");
        binder.bind(DynamoAwsMetadataProvider.class).to(LiveDynamoAwsMetadataProvider.class);

        binder.bind(DynamoConnectorId.class).toInstance(new DynamoConnectorId(connectorId));
        binder.bind(DynamoConnector.class).in(Scopes.SINGLETON);
        binder.bind(DynamoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DynamoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DynamoRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(DynamoHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(DynamoConnectorRecordSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(LiveDynamoAwsMetadataProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(LiveDynamoAwsMetadataProvider.class).as(generatedNameOf(LiveDynamoAwsMetadataProvider.class, connectorId));

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
    }

    @ForDynamo
    @Singleton
    @Provides
    public static ExecutorService createCachingDynamoSchemaExecutor(DynamoConnectorId clientId, DynamoClientConfig dynamoClientConfig) {
        return newFixedThreadPool(
                dynamoClientConfig.getMaxSchemaRefreshThreads(),
                daemonThreadsNamed("dynamo-" + clientId + "-%s"));
    }

    @Singleton
    @Provides
    public static DynamoSession createDynamoSession(
            DynamoConnectorId connectorId,
            DynamoClientConfig config,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec) {
        checkNotNull(config, "config is null");
        checkNotNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        // TODO load DynamoMetadata from file
        return new DynamoSession(
                connectorId.toString(),
                "",
                new Identity(Optional.ofNullable(config.getUsername()).orElse("user"), Optional.empty()),
                TimeZoneKey.UTC_KEY,
                Locale.ENGLISH,
                System.currentTimeMillis(), ImmutableMap.of(),
                new DynamoAwsMetadata());
    }
}
