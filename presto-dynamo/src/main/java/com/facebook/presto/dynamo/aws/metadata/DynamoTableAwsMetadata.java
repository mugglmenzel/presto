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

public class DynamoTableAwsMetadata
{
    private String region;
    private String tableName;
    private List<DynamoColumnAwsMetadata> columns;

    public DynamoTableAwsMetadata()
    {
        this.columns = new ArrayList<DynamoColumnAwsMetadata>();
    }

    public DynamoTableAwsMetadata(String region, String tableName,
            List<DynamoColumnAwsMetadata> columns)
    {
        this.region = region;
        this.tableName = tableName;
        this.columns = columns == null ? new ArrayList<DynamoColumnAwsMetadata>()
                : columns;
    }

    public String getRegion()
    {
        return region;
    }

    public void setRegion(String region)
    {
        this.region = region;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public List<DynamoColumnAwsMetadata> getColumns()
    {
        return columns;
    }

    public void setColumns(List<DynamoColumnAwsMetadata> columns)
    {
        this.columns = columns == null ? new ArrayList<DynamoColumnAwsMetadata>()
                : columns;
    }

    @Override
    public String toString() {
        return "DynamoTableAwsMetadata{" +
                "region='" + region + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
