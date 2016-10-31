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

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.presto.dynamo.aws.metadata.CachingDynamoAwsMetadataProvider;
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadata;
import com.google.common.util.concurrent.ListeningExecutorService;

@Test(singleThreaded = true)
public class TestCachingDynamoMetadataProvider
{
    private static final String CONNECTOR_ID = "test-dynamo";
    private MockDynamoSession mockSession;
    private CachingDynamoAwsMetadataProvider schemaProvider;

    @BeforeMethod
    public void setUp() throws Exception
    {
        String filePath = DynamoTestingUtils.createTestMetadataFile();

        DynamoClientConfig config = new DynamoClientConfig();
        config.setMetadataFile(filePath);

        mockSession = new MockDynamoSession(CONNECTOR_ID, config);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        schemaProvider = new CachingDynamoAwsMetadataProvider(executor,
                new Duration(5, TimeUnit.MINUTES), new Duration(1,
                        TimeUnit.MINUTES), filePath);
    }

    @Test
    public void testGetMetadata() throws Exception
    {
        assertEquals(schemaProvider.getMetadata().getClass(),
                DynamoAwsMetadata.class);
        schemaProvider.flushCache();

        assertEquals(schemaProvider.getMetadata().getClass(),
                DynamoAwsMetadata.class);
    }

    @Test
    public void testNoCacheExceptions() throws Exception
    {
        // Throw exceptions on usage
        mockSession.setThrowException(true);
        try {
            schemaProvider.getMetadata();
        }
        catch (RuntimeException ignored) {
        }

        // Second try should hit the client again
        try {
            schemaProvider.getMetadata();
        }
        catch (RuntimeException ignored) {
        }
    }
}
