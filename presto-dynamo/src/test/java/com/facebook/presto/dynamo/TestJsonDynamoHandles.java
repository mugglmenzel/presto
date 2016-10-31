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

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.facebook.presto.dynamo.type.DynamoType;
import io.airlift.json.ObjectMapperProvider;

import java.util.Map;

import org.testng.annotations.Test;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Test
public class TestJsonDynamoHandles
{
    private static final Map<String, Object> TABLE_HANDLE_AS_MAP = ImmutableMap.<String, Object>of(
            "connectorId", "dynamo",
            "schemaName", "dynamo_schema",
            "tableName", "dynamo_table");

    private static final Map<String, Object> COLUMN_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("connectorId", "dynamo")
            .put("name", "column")
            .put("ordinalPosition", 42)
            .put("dynamoType", "LONG")
            .put("hashKey", false)
            .put("rangeKey", true)
            .put("indexed", false)
            .put("hidden", false)
            .build();

    private static final Map<String, Object> COLUMN2_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("connectorId", "dynamo")
            .put("name", "column2")
            .put("ordinalPosition", 0)
            .put("dynamoType", "SET")
            .put("typeArguments", ImmutableList.of("LONG"))
            .put("hashKey", false)
            .put("rangeKey", false)
            .put("indexed", false)
            .put("hidden", false)
            .build();

    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testTableHandleSerialize()
            throws Exception
    {
        DynamoTableHandle tableHandle = new DynamoTableHandle("dynamo", "dynamo_schema", "dynamo_table");

        assertTrue(objectMapper.canSerialize(DynamoTableHandle.class));
        String json = objectMapper.writeValueAsString(tableHandle);
        testJsonEquals(json, TABLE_HANDLE_AS_MAP);
    }

    @Test
    public void testTableHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(TABLE_HANDLE_AS_MAP);

        DynamoTableHandle tableHandle = objectMapper.readValue(json, DynamoTableHandle.class);

        assertEquals(tableHandle.getConnectorId(), "dynamo");
        assertEquals(tableHandle.getSchemaName(), "dynamo_schema");
        assertEquals(tableHandle.getTableName(), "dynamo_table");
        assertEquals(tableHandle.getSchemaTableName(), new SchemaTableName("dynamo_schema", "dynamo_table"));
    }

    @Test
    public void testColumnHandleSerialize()
            throws Exception
    {
        DynamoColumnHandle columnHandle = new DynamoColumnHandle("dynamo", "column", 42, DynamoType.LONG, null, false, true, false, false);

        assertTrue(objectMapper.canSerialize(DynamoColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN_HANDLE_AS_MAP);
    }

    @Test
    public void testColumn2HandleSerialize()
            throws Exception
    {
        DynamoColumnHandle columnHandle = new DynamoColumnHandle(
                "dynamo",
                "column2",
                0,
                DynamoType.SET,
                ImmutableList.of(DynamoType.LONG),
                false,
                false,
                false,
                false);

        assertTrue(objectMapper.canSerialize(DynamoColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN2_HANDLE_AS_MAP);
    }

    @Test
    public void testColumnHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN_HANDLE_AS_MAP);

        DynamoColumnHandle columnHandle = objectMapper.readValue(json, DynamoColumnHandle.class);

        assertEquals(columnHandle.getName(), "column");
        assertEquals(columnHandle.getOrdinalPosition(), 42);
        assertEquals(columnHandle.getDynamoType(), DynamoType.LONG);
        assertEquals(columnHandle.getTypeArguments(), null);
        assertEquals(columnHandle.isHashKey(), false);
        assertEquals(columnHandle.isRangeKey(), true);
    }

    @Test
    public void testColumn2HandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN2_HANDLE_AS_MAP);

        DynamoColumnHandle columnHandle = objectMapper.readValue(json, DynamoColumnHandle.class);

        assertEquals(columnHandle.getName(), "column2");
        assertEquals(columnHandle.getOrdinalPosition(), 0);
        assertEquals(columnHandle.getDynamoType(), DynamoType.SET);
        assertEquals(columnHandle.getTypeArguments(), ImmutableList.of(DynamoType.LONG));
        assertEquals(columnHandle.isHashKey(), false);
        assertEquals(columnHandle.isRangeKey(), false);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
