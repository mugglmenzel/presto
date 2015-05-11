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

import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Predicate;

public class DynamoColumnHandle
        implements ColumnHandle
{
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "presto_sample_weight";

    private final String connectorId;
    private final String name;
    private final int ordinalPosition;
    private final DynamoType dynamoType;
    private final List<DynamoType> typeArguments;
    private final boolean hashKey;
    private final boolean rangeKey;
    private final boolean indexed;
    private final boolean hidden;

    @JsonCreator
    public DynamoColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("dynamoType") DynamoType dynamoType,
            @Nullable @JsonProperty("typeArguments") List<DynamoType> typeArguments,
            @JsonProperty("hashKey") boolean hashKey,
            @JsonProperty("rangeKey") boolean rangeKey,
            @JsonProperty("indexed") boolean indexed,
            @JsonProperty("hidden") boolean hidden)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.name = checkNotNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        this.dynamoType = checkNotNull(dynamoType, "dynamoType is null");
        int typeArgsSize = dynamoType.getTypeArgumentSize();
        if (typeArgsSize > 0) {
            this.typeArguments = checkNotNull(typeArguments, "typeArguments is null");
            checkArgument(typeArguments.size() == typeArgsSize, dynamoType
                    + " must provide " + typeArgsSize + " type arguments");
        }
        else {
            this.typeArguments = null;
        }
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.indexed = indexed;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public DynamoType getDynamoType()
    {
        return dynamoType;
    }

    @JsonProperty
    public List<DynamoType> getTypeArguments()
    {
        return typeArguments;
    }

    @JsonProperty
    public boolean isHashKey()
    {
        return hashKey;
    }

    @JsonProperty
    public boolean isRangeKey()
    {
        return rangeKey;
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadataCaseSensitive(name, dynamoType.getNativeType(), ordinalPosition, hashKey, null, hidden);
    }

    public Type getType()
    {
        return dynamoType.getNativeType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                name,
                ordinalPosition,
                dynamoType,
                typeArguments,
                hashKey,
                rangeKey,
                indexed,
                hidden);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DynamoColumnHandle other = (DynamoColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.dynamoType, other.dynamoType) &&
                Objects.equals(this.typeArguments, other.typeArguments) &&
                Objects.equals(this.hashKey, other.hashKey) &&
                Objects.equals(this.rangeKey, other.rangeKey) &&
                Objects.equals(this.indexed, other.indexed) &&
                Objects.equals(this.hidden, other.hidden);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("dynamoType", dynamoType);

        if (typeArguments != null && !typeArguments.isEmpty()) {
            helper.add("typeArguments", typeArguments);
        }

        helper.add("hashKey", hashKey)
                .add("rangeKey", rangeKey)
                .add("indexed", indexed)
                .add("hidden", hidden);

        return helper.toString();
    }

    public static Function<ColumnHandle, DynamoColumnHandle> dynamoColumnHandle()
    {
        return new Function<ColumnHandle, DynamoColumnHandle>()
        {
            @Override
            public DynamoColumnHandle apply(ColumnHandle columnHandle)
            {
                return checkType(columnHandle, DynamoColumnHandle.class, "columnHandle");
            }
        };
    }

    public static Function<ColumnHandle, ColumnMetadata> columnMetadataGetter()
    {
        return new Function<ColumnHandle, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(ColumnHandle columnHandle)
            {
                checkNotNull(columnHandle, "columnHandle is null");
                checkArgument(columnHandle instanceof DynamoColumnHandle,
                        "columnHandle is not an instance of DynamoColumnHandle");
                return ((DynamoColumnHandle) columnHandle).getColumnMetadata();
            }
        };
    }

    public static Function<DynamoColumnHandle, Type> nativeTypeGetter()
    {
        return new Function<DynamoColumnHandle, Type>()
        {
            @Override
            public Type apply(DynamoColumnHandle input)
            {
                return input.getType();
            }
        };
    }

    public static Function<DynamoColumnHandle, FullDynamoType> dynamoFullTypeGetter()
    {
        return new Function<DynamoColumnHandle, FullDynamoType>()
        {
            @Override
            public FullDynamoType apply(DynamoColumnHandle input)
            {
                if (input.getDynamoType().getTypeArgumentSize() == 0) {
                    return input.getDynamoType();
                }
                else {
                    return new DynamoTypeWithTypeArguments(input.getDynamoType(), input.getTypeArguments());
                }
            }
        };
    }

    public static Predicate<DynamoColumnHandle> hashKeyPredicate()
    {
        return new Predicate<DynamoColumnHandle>()
        {
            @Override
            public boolean apply(DynamoColumnHandle columnHandle)
            {
                return columnHandle.isHashKey();
            }
        };
    }
}
