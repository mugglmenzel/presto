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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestDynamoClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DynamoClientConfig.class)
                .setLimitForPartitionKeySelect(200)
                .setFetchSizeForPartitionKeySelect(20_000)
                .setMaxSchemaRefreshThreads(10)
                .setSchemaCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setSchemaRefreshInterval(new Duration(2, TimeUnit.MINUTES))
                .setFetchSize(5_000)
                .setNativeProtocolPort(9042)
                .setSplitSize(1_024)
                .setMinSplitCount(2)
                .setTransportFactoryOptions("")
                .setAllowDropTable(false)
                .setUsername(null)
                .setPassword(null)
                .setClientReadTimeout(new Duration(30000, MILLISECONDS))
                .setClientConnectTimeout(new Duration(30000, MILLISECONDS))
                .setClientSoLinger(null)
                .setMetadataFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("dynamo.limit-for-partition-key-select", "100")
                .put("dynamo.fetch-size-for-partition-key-select", "500")
                .put("dynamo.max-schema-refresh-threads", "2")
                .put("dynamo.schema-cache-ttl", "2h")
                .put("dynamo.schema-refresh-interval", "30m")
                .put("dynamo.native-protocol-port", "9999")
                .put("dynamo.fetch-size", "10000")
                .put("dynamo.split-size", "1025")
                .put("dynamo.min-split-count", "5")
                .put("dynamo.transport-factory-options", "a=b")
                .put("dynamo.allow-drop-table", "true")
                .put("dynamo.username", "my_username")
                .put("dynamo.password", "my_password")
                .put("dynamo.client.read-timeout", "11ms")
                .put("dynamo.client.connect-timeout", "22ms")
                .put("dynamo.client.so-linger", "33")
                .put("dynamo.metadata-file", "/tmp/dynamo-metadata.json")
                .build();

        DynamoClientConfig expected = new DynamoClientConfig()
                .setLimitForPartitionKeySelect(100)
                .setFetchSizeForPartitionKeySelect(500)
                .setMaxSchemaRefreshThreads(2)
                .setSchemaCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setSchemaRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setNativeProtocolPort(9999)
                .setFetchSize(10_000)
                .setSplitSize(1_025)
                .setMinSplitCount(5)
                .setTransportFactoryOptions("a=b")
                .setAllowDropTable(true)
                .setUsername("my_username")
                .setPassword("my_password")
                .setClientReadTimeout(new Duration(11, MILLISECONDS))
                .setClientConnectTimeout(new Duration(22, MILLISECONDS))
                .setClientSoLinger(33)
                .setMetadataFile("/tmp/dynamo-metadata.json");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
