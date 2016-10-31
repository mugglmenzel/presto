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

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

import com.facebook.presto.dynamo.type.DynamoType;
import io.airlift.json.JsonCodec;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class TestDynamoColumnHandle
{
    private final JsonCodec<DynamoColumnHandle> codec = jsonCodec(DynamoColumnHandle.class);

    @Test
    public void testRoundTrip()
    {
        DynamoColumnHandle expected = new DynamoColumnHandle("connector", "name", 42, DynamoType.STRING, null, true, false, false, false);

        String json = codec.toJson(expected);
        DynamoColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getDynamoType(), expected.getDynamoType());
        assertEquals(actual.isHashKey(), expected.isHashKey());
        assertEquals(actual.isRangeKey(), expected.isRangeKey());
    }

    @Test
    public void testRoundTrip2()
    {
        DynamoColumnHandle expected = new DynamoColumnHandle(
                "connector",
                "name2",
                1,
                DynamoType.MAP,
                ImmutableList.of(DynamoType.STRING, DynamoType.STRING),
                false,
                true,
                false,
                false);

        String json = codec.toJson(expected);
        DynamoColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getDynamoType(), expected.getDynamoType());
        assertEquals(actual.isHashKey(), expected.isHashKey());
        assertEquals(actual.isRangeKey(), expected.isRangeKey());
    }
}
