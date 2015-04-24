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
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;

public class DynamoTokenSplitManager
{
    private final ExecutorService executor;
    private final int splitSize;

    @Inject
    public DynamoTokenSplitManager(@ForDynamo ExecutorService executor, DynamoClientConfig config)
    {
        this.executor = checkNotNull(executor, "executor is null");
        this.splitSize = config.getSplitSize();
    }

    public List<TokenSplit> getSplits(String keyspace, String columnFamily)
            throws IOException
    {
        List<TokenSplit> splits = new ArrayList<TokenSplit>();
        splits.add(new TokenSplit("", "", new ArrayList<String>()));

        checkState(!splits.isEmpty(), "No splits created");
        //noinspection SharedThreadLocalRandom
        Collections.shuffle(splits, ThreadLocalRandom.current());
        return splits;
    }

    public static class TokenSplit
    {
        private String startToken;
        private String endToken;
        private List<String> hosts;

        public TokenSplit(String startToken, String endToken, List<String> hosts)
        {
            this.startToken = checkNotNull(startToken, "startToken is null");
            this.endToken = checkNotNull(endToken, "endToken is null");
            this.hosts = ImmutableList.copyOf(checkNotNull(hosts, "hosts is null"));
        }

        public String getStartToken()
        {
            return startToken;
        }

        public String getEndToken()
        {
            return endToken;
        }

        public List<String> getHosts()
        {
            return hosts;
        }
    }
}
