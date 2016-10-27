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

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamoClientConfig {
    private Duration schemaCacheTtl = new Duration(1, TimeUnit.HOURS);
    private Duration schemaRefreshInterval = new Duration(2, TimeUnit.MINUTES);
    private int maxSchemaRefreshThreads = 10;
    private int limitForPartitionKeySelect = 200;
    private int fetchSizeForPartitionKeySelect = 20_000;
    private int fetchSize = 5_000;
    private int nativeProtocolPort = 9042;
    private int partitionSizeForBatchSelect = 100;
    private int splitSize = 1_024;
    private int minSplitCount = 2;
    private Map<String, String> transportFactoryOptions = new HashMap<>();
    private boolean allowDropTable;
    private String username;
    private String password;
    private Duration clientReadTimeout = new Duration(
            30000, MILLISECONDS);
    private Duration clientConnectTimeout = new Duration(
            30000, MILLISECONDS);
    private Integer clientSoLinger;
    private String metadataFile;

    @Min(0)
    public int getLimitForPartitionKeySelect() {
        return limitForPartitionKeySelect;
    }

    @Config("dynamo.limit-for-partition-key-select")
    public DynamoClientConfig setLimitForPartitionKeySelect(
            int limitForPartitionKeySelect) {
        this.limitForPartitionKeySelect = limitForPartitionKeySelect;
        return this;
    }

    @Min(1)
    public int getMaxSchemaRefreshThreads() {
        return maxSchemaRefreshThreads;
    }

    @Config("dynamo.max-schema-refresh-threads")
    public DynamoClientConfig setMaxSchemaRefreshThreads(
            int maxSchemaRefreshThreads) {
        this.maxSchemaRefreshThreads = maxSchemaRefreshThreads;
        return this;
    }

    @NotNull
    public Duration getSchemaCacheTtl() {
        return schemaCacheTtl;
    }

    @Config("dynamo.schema-cache-ttl")
    public DynamoClientConfig setSchemaCacheTtl(Duration schemaCacheTtl) {
        this.schemaCacheTtl = schemaCacheTtl;
        return this;
    }

    @NotNull
    public Duration getSchemaRefreshInterval() {
        return schemaRefreshInterval;
    }

    @Config("dynamo.schema-refresh-interval")
    public DynamoClientConfig setSchemaRefreshInterval(
            Duration schemaRefreshInterval) {
        this.schemaRefreshInterval = schemaRefreshInterval;
        return this;
    }

    @Min(1)
    public int getNativeProtocolPort() {
        return nativeProtocolPort;
    }

    @Config(("dynamo.native-protocol-port"))
    public DynamoClientConfig setNativeProtocolPort(int nativeProtocolPort) {
        this.nativeProtocolPort = nativeProtocolPort;
        return this;
    }

    @Min(1)
    public int getFetchSize() {
        return fetchSize;
    }

    @Config("dynamo.fetch-size")
    public DynamoClientConfig setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    @Min(1)
    public int getFetchSizeForPartitionKeySelect() {
        return fetchSizeForPartitionKeySelect;
    }

    @Config("dynamo.fetch-size-for-partition-key-select")
    public DynamoClientConfig setFetchSizeForPartitionKeySelect(
            int fetchSizeForPartitionKeySelect) {
        this.fetchSizeForPartitionKeySelect = fetchSizeForPartitionKeySelect;
        return this;
    }

    @Min(1)
    public int getPartitionSizeForBatchSelect() {
        return partitionSizeForBatchSelect;
    }

    @Config("dynamo.partition-size-for-batch-select")
    public DynamoClientConfig setPartitionSizeForBatchSelect(
            int partitionSizeForBatchSelect) {
        this.partitionSizeForBatchSelect = partitionSizeForBatchSelect;
        return this;
    }

    @Min(1)
    public int getSplitSize() {
        return splitSize;
    }

    @Config("dynamo.split-size")
    public DynamoClientConfig setSplitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    @Min(1)
    public int getMinSplitCount() {
        return minSplitCount;
    }

    @Config("dynamo.min-split-count")
    public DynamoClientConfig setMinSplitCount(int minSplitCount) {
        this.minSplitCount = minSplitCount;
        return this;
    }

    public Map<String, String> getTransportFactoryOptions() {
        return transportFactoryOptions;
    }

    @Config("dynamo.transport-factory-options")
    public DynamoClientConfig setTransportFactoryOptions(
            String transportFactoryOptions) {
        checkNotNull(transportFactoryOptions, "transportFactoryOptions is null");
        this.transportFactoryOptions = Splitter.on(',').omitEmptyStrings()
                .trimResults().withKeyValueSeparator("=")
                .split(transportFactoryOptions);
        return this;
    }

    public boolean getAllowDropTable() {
        return this.allowDropTable;
    }

    @Config("dynamo.allow-drop-table")
    @ConfigDescription("Allow hive connector to drop table")
    public DynamoClientConfig setAllowDropTable(boolean allowDropTable) {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public String getUsername() {
        return username;
    }

    @Config("dynamo.username")
    public DynamoClientConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("dynamo.password")
    public DynamoClientConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientReadTimeout() {
        return clientReadTimeout;
    }

    @Config("dynamo.client.read-timeout")
    public DynamoClientConfig setClientReadTimeout(Duration clientReadTimeout) {
        this.clientReadTimeout = clientReadTimeout;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientConnectTimeout() {
        return clientConnectTimeout;
    }

    @Config("dynamo.client.connect-timeout")
    public DynamoClientConfig setClientConnectTimeout(
            Duration clientConnectTimeout) {
        this.clientConnectTimeout = clientConnectTimeout;
        return this;
    }

    @Min(0)
    public Integer getClientSoLinger() {
        return clientSoLinger;
    }

    @Config("dynamo.client.so-linger")
    public DynamoClientConfig setClientSoLinger(Integer clientSoLinger) {
        this.clientSoLinger = clientSoLinger;
        return this;
    }

    public String getMetadataFile() {
        return metadataFile;
    }

    @Config("dynamo.metadata-file")
    public DynamoClientConfig setMetadataFile(String metadataFile) {
        this.metadataFile = metadataFile;
        return this;
    }
}
