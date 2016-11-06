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
package com.facebook.presto.dynamo.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.dynamo.util.DynamoUtils;
import com.facebook.presto.spi.type.*;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public enum DynamoType implements FullDynamoType {
    STRING(VarcharType.VARCHAR, String.class),
    LONG(BigintType.BIGINT, Long.class),
    DOUBLE(DoubleType.DOUBLE, Double.class),
    BINARY(VarcharType.VARCHAR, ByteBuffer.class),
    BOOLEAN(BooleanType.BOOLEAN, Boolean.class),
    LIST(VarcharType.VARCHAR, null),
    MAP(VarcharType.VARCHAR, null),
    SET(VarcharType.VARCHAR, null);

    private final Type nativeType;
    private final Class<?> javaType;

    private static final Logger Log = Logger.get(DynamoType.class);

    DynamoType(Type nativeType, Class<?> javaType) {
        this.nativeType = checkNotNull(nativeType, "nativeType is null");
        this.javaType = javaType;
    }

    public Type getNativeType() {
        return nativeType;
    }

    public int getTypeArgumentSize() {
        switch (this) {
            case LIST:
            case SET:
                return 1;
            case MAP:
                return 2;
            default:
                return 0;
        }
    }

    public static DynamoType getDynamoType(String name) {
        switch (name) {
            case "String":
                return STRING;
            case "Long":
                return LONG;
            case "Integer":
                return LONG;
            case "Double":
                return DOUBLE;
            case "Float":
                return DOUBLE;
            default:
                return null;
        }
    }

    public static DynamoType getSupportedDynamoType(String dynamoTypeName) {
        DynamoType dynamoType = getDynamoType(dynamoTypeName);
        checkArgument(dynamoType != null, "Unknown Dynamo type: "
                + dynamoTypeName);
        return dynamoType;
    }

    public static Comparable<?> getColumnValue(Map<String, AttributeValue> row,
                                               String columnName, FullDynamoType fullDynamoType) {
        return getColumnValue(row, columnName, fullDynamoType.getDynamoType(),
                fullDynamoType.getTypeArguments());
    }

    public static Comparable<?> getColumnValue(Map<String, AttributeValue> row,
                                               String columnName, DynamoType dynamoType,
                                               List<DynamoType> typeArguments) {
        Log.info("Getting column value for column " + columnName + " from row " + row);
        String key = columnName;
        AttributeValue attValue = row.keySet().stream().filter(c -> c.equalsIgnoreCase(columnName)).findFirst().map(row::get).orElse(null);
        Log.info("Got column value " + attValue);
        if (attValue == null
                || (attValue.isNULL() != null && attValue.isNULL())) {
            return null;
        } else {
            switch (dynamoType) {
                case STRING:
                    return attValue.getS();
                case LONG:
                    return Long.parseLong(attValue.getN());
                case DOUBLE:
                    return Double.parseDouble(attValue.getN());
                case BOOLEAN:
                    return attValue.getBOOL();
                case BINARY:
                    return attValue.getB();
                case SET:
                    checkTypeArguments(dynamoType, 1, typeArguments);
                    return buildSetValue(row, key, typeArguments.get(0));
                case LIST:
                    checkTypeArguments(dynamoType, 1, typeArguments);
                    return buildListValue(row, key, typeArguments.get(0));
                case MAP:
                    checkTypeArguments(dynamoType, 2, typeArguments);
                    return buildMapValue(row, key, typeArguments.get(0),
                            typeArguments.get(1));
                default:
                    throw new IllegalStateException("Handling of type "
                            + dynamoType + " is not implemented");
            }
        }
    }

    private static String buildSetValue(Map<String, AttributeValue> row,
                                        String name, DynamoType elemType) {
        AttributeValue v = row.get(name);
        List<String> values = null;
        if (v == null || v.getNS() == null) {
            values = new ArrayList<String>();
        } else {
            values = v.getNS();
        }
        return buildArrayValue(values, elemType);
    }

    private static String buildListValue(Map<String, AttributeValue> row,
                                         String name, DynamoType elemType) {
        AttributeValue v = row.get(name);
        List<String> values = null;
        if (v == null || v.getNS() == null) {
            values = new ArrayList<String>();
        } else {
            values = v.getNS();
        }
        return buildArrayValue(values, elemType);
    }

    private static String buildMapValue(Map<String, AttributeValue> row,
                                        String name, DynamoType keyType, DynamoType valueType) {
        AttributeValue v = row.get(name);
        Map<String, AttributeValue> values = null;
        if (v == null || v.getNS() == null) {
            values = new HashMap<String, AttributeValue>();
        } else {
            values = v.getM();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    @VisibleForTesting
    public static String buildArrayValue(Collection<?> collection, DynamoType elemType) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elemType));
        }
        sb.append("]");
        return sb.toString();
    }

    private static void checkTypeArguments(DynamoType type, int expectedSize,
                                           List<DynamoType> typeArguments) {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException(
                    "Wrong number of type arguments " + typeArguments + " for "
                            + type);
        }
    }

    private static String objectToString(Object object, DynamoType elemType) {
        switch (elemType) {
            case STRING:
            case LONG:
            case DOUBLE:
                return DynamoUtils.quoteStringLiteralForJson(object.toString());

            case BINARY:
                return DynamoUtils.quoteStringLiteralForJson(Hex
                        .encodeHexString(((ByteBuffer) object).array()));

            case BOOLEAN:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType
                        + " is not implemented");
        }
    }

    @Override
    public DynamoType getDynamoType() {
        if (getTypeArgumentSize() == 0) {
            return this;
        } else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    @Override
    public List<DynamoType> getTypeArguments() {
        if (getTypeArgumentSize() == 0) {
            return null;
        } else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    public Object getJavaValue(Comparable<?> comparable) {
        switch (this) {
            case STRING:
            case LONG:
            case DOUBLE:
            case BOOLEAN:
                return comparable;
            case BINARY:
                try {
                    return ByteBuffer.wrap(Hex.decodeHex(((String) comparable)
                            .toCharArray()));
                } catch (DecoderException e) {
                    throw new RuntimeException(e);
                }
            case SET:
            case LIST:
            case MAP:
            default:
                throw new IllegalStateException(
                        "Back conversion not implemented for " + this);
        }
    }

    public static DynamoType toDynamoType(Type type) {
        if (type.equals(BooleanType.BOOLEAN)) {
            return BOOLEAN;
        } else if (type.equals(BigintType.BIGINT)) {
            return LONG;
        } else if (type.equals(DoubleType.DOUBLE)) {
            return DOUBLE;
        } else if (type.equals(VarcharType.VARCHAR)) {
            return STRING;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
