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
package com.facebook.presto.dynamo.aws.metadata;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.dynamo.type.DynamoType;

public class DynamoColumnAwsMetadata
{
    private String columnName;
    private DynamoType columnType;
    private List<DynamoType> typeArguments;

    public DynamoColumnAwsMetadata()
    {
        this.typeArguments = new ArrayList<DynamoType>();
    }

    public DynamoColumnAwsMetadata(String columnName, DynamoType columnType,
            List<DynamoType> typeArguments)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.typeArguments = typeArguments == null ? new ArrayList<DynamoType>()
                : typeArguments;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public DynamoType getColumnType()
    {
        return columnType;
    }

    public void setColumnType(DynamoType columnType)
    {
        this.columnType = columnType;
    }

    public List<DynamoType> getTypeArguments()
    {
        return typeArguments;
    }

    public void setTypeArguments(List<DynamoType> typeArguments)
    {
        this.typeArguments = typeArguments;
    }

    @Override
    public String toString() {
        return "DynamoColumnAwsMetadata{" +
                "columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                ", typeArguments=" + typeArguments +
                '}';
    }
}
