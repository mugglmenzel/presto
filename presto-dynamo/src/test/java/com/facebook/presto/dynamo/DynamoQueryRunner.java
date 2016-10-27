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

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

public final class DynamoQueryRunner
{
    private DynamoQueryRunner()
    {
    }

    private static final String TEST_SCHEMA = "us_west_1";

    public static DistributedQueryRunner createDynamoQueryRunner()
            throws Exception
    {
        String metadataFilePath = DynamoTestingUtils.createTestMetadataFile();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 4);

        queryRunner.installPlugin(new DynamoPlugin());
        queryRunner.createCatalog("dynamo", "dynamo", ImmutableMap.of(
                "dynamo.metadata-file", metadataFilePath));

        return queryRunner;
    }

    public static Session createSession()
    {
        return createDynamoSession(TEST_SCHEMA);
    }

    public static Session createDynamoSession(String schema)
    {
        return Session.builder(new SessionPropertyManager())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("test")
                .setCatalog("dynamo")
                .setSchema(schema)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }
}
