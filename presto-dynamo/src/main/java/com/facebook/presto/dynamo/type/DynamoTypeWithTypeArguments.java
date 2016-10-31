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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

public class DynamoTypeWithTypeArguments
        implements FullDynamoType
{
    private final DynamoType dynamoType;
    private final List<DynamoType> typeArguments;

    public DynamoTypeWithTypeArguments(DynamoType dynamoType, List<DynamoType> typeArguments)
    {
        this.dynamoType = checkNotNull(dynamoType, "dynamoType is null");
        this.typeArguments = checkNotNull(typeArguments, "typeArguments is null");
    }

    @Override
    public DynamoType getDynamoType()
    {
        return dynamoType;
    }

    @Override
    public List<DynamoType> getTypeArguments()
    {
        return typeArguments;
    }

    @Override
    public String toString()
    {
        if (typeArguments != null) {
            return dynamoType.toString() + typeArguments.toString();
        }
        else {
            return dynamoType.toString();
        }
    }
}
