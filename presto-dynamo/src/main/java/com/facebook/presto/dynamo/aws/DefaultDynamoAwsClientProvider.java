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
package com.facebook.presto.dynamo.aws;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.Map;

public class DefaultDynamoAwsClientProvider implements DynamoAwsClientProvider {
    private static final Logger log = Logger.get(DefaultDynamoAwsClientProvider.class);

    private Map<String, AmazonDynamoDB> clientCache = new HashMap<String, AmazonDynamoDB>();

    public synchronized AmazonDynamoDB getClient(String region) {
        String clientKey = region.toLowerCase();
        AmazonDynamoDB client = clientCache.get(clientKey);
        if (client != null) {
            return client;
        }

        client = createClient(region);
        clientCache.put(clientKey, client);
        return client;
    }

    private AmazonDynamoDB createClient(String region) {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(
                new DefaultAWSCredentialsProviderChain());
        Regions regionEnum = AwsUtils.getRegionByEnumName(region);
        client.setRegion(RegionUtils.getRegion(regionEnum.getName()));
        log.info(String.format("Created DynamoDB client with region %s", regionEnum));
        return client;
    }
}
