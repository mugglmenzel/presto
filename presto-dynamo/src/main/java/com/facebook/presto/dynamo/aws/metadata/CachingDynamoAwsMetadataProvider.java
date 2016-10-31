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

import static com.facebook.presto.dynamo.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.weakref.jmx.Managed;

import com.facebook.presto.dynamo.DynamoClientConfig;
import com.facebook.presto.dynamo.ForDynamo;
import com.facebook.presto.spi.NotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@ThreadSafe
public class CachingDynamoAwsMetadataProvider implements DynamoAwsMetadataProvider
{
    private static final Logger log = Logger
            .get(CachingDynamoAwsMetadataProvider.class);

    private String metadataFilePath;

    // cache key is empty string
    private final LoadingCache<String, DynamoAwsMetadata> metadataCache;

    /**
     * Mapping from an empty string to all schema names. Each schema name is a
     * mapping from the lower case schema name to the case sensitive schema
     * name. This mapping is necessary because Presto currently does not
     * properly handle case sensitive names.
     */
    // private final LoadingCache<String, Map<String, String>> schemaNamesCache;

    @Inject
    public CachingDynamoAwsMetadataProvider(
            @ForDynamo ExecutorService executor, DynamoClientConfig config)
    {
        this(executor, checkNotNull(config,
                "config (DynamoClientConfig) is null").getSchemaCacheTtl(),
                config.getSchemaRefreshInterval(), config.getMetadataFile());
    }

    public CachingDynamoAwsMetadataProvider(ExecutorService executor,
            Duration cacheTtl, Duration refreshInterval, String metadataFilePath)
    {
        checkNotNull(executor, "executor is null");
        checkNotNull(metadataFilePath, "metadataFilePath is null");

        this.metadataFilePath = metadataFilePath;

        long expiresAfterWriteMillis = checkNotNull(cacheTtl,
                "cacheTtl is null").toMillis();
        long refreshMills = checkNotNull(refreshInterval,
                "refreshInterval is null").toMillis();

        metadataCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .refreshAfterWrite(refreshMills, MILLISECONDS)
                .build(asyncReloading(new CacheLoader<String, DynamoAwsMetadata>()
                {
                    @Override
                    public DynamoAwsMetadata load(String key) throws Exception
                    {
                        return loadMetadata();
                    }
                }, executor));
    }

    @Managed
    public void flushCache()
    {
        metadataCache.invalidateAll();
    }

    private DynamoAwsMetadata loadMetadata() throws Exception
    {
        return retry()
                .stopOn(NotFoundException.class)
                .stopOnIllegalExceptions()
                .run("loadMetadata",
                        () ->
                        {
                            ObjectMapper mapper = new ObjectMapper();
                            File file = new File(metadataFilePath);
                            DynamoAwsMetadata metadata = mapper.readValue(file,
                                    DynamoAwsMetadata.class);
                            return metadata;
                        });
    }

    @Override
    public DynamoAwsMetadata getMetadata()
    {
        try {
            return this.metadataCache.get("");
        }
        catch (ExecutionException e) {
            log.warn(e, "Failed to load metadata");
            return new DynamoAwsMetadata();
        }
    }
}
