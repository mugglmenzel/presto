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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.facebook.presto.dynamo.aws.AwsUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;

//JSON Example
//{
//    "tables": [
//        {
//            "columns": [
//                {
//                    "columnName": "UserId",
//                    "columnType": "STRING",
//                    "typeArguments": []
//                },
//                {
//                    "columnName": "Version",
//                    "columnType": "LONG",
//                    "typeArguments": []
//                }
//            ],
//            "region": "us_west_2",
//            "tableName": "Users"
//        },
//        {
//            "columns": [
//                {
//                    "columnName": "BookName",
//                    "columnType": "STRING",
//                    "typeArguments": []
//                },
//                {
//                    "columnName": "Writers",
//                    "columnType": "LIST",
//                    "typeArguments": [
//                        "STRING"
//                    ]
//                }
//            ],
//            "region": "us_west_2",
//            "tableName": "Books"
//        }
//    ]
//}

public class DynamoAwsMetadata
{
    private Logger Log = Logger.get(DynamoAwsMetadata.class);
    private List<DynamoTableAwsMetadata> tables;

    public DynamoAwsMetadata()
    {
        this.tables = new ArrayList<DynamoTableAwsMetadata>();
    }

    @JsonProperty
    public List<DynamoTableAwsMetadata> getTables()
    {
        return tables;
    }

    public DynamoAwsMetadata setTables(List<DynamoTableAwsMetadata> tables)
    {
        if (tables == null) {
            tables = new ArrayList<DynamoTableAwsMetadata>();
        }

        this.tables = tables;
        return this;
    }

    public List<String> getRegionsAsSchemaNames()
    {
        Set<String> set = new HashSet<String>();
        for (DynamoTableAwsMetadata entry : tables) {
            set.add(AwsUtils.getRegionAsSchemaName(entry.getRegion()));
        }
        return new ArrayList<String>(set);
    }

    public List<String> getTableNames(String region)
    {
        Set<String> set = new HashSet<String>();
        for (DynamoTableAwsMetadata entry : tables) {
            if (entry.getRegion().equalsIgnoreCase(region)) {
                set.add(entry.getTableName());
            }
        }
        return new ArrayList<String>(set);
    }

    public DynamoTableAwsMetadata getTable(String region, String tableName)
    {
        Log.info("Returning metadata for table " + tableName);
        for (DynamoTableAwsMetadata entry : tables) {
            if (entry.getRegion().equalsIgnoreCase(region)
                    && entry.getTableName().equalsIgnoreCase(tableName)) {
                return entry;
            }
        }
        return null;
    }

    public String getAwsTableName(String region, String tableName)
    {
        for (DynamoTableAwsMetadata entry : tables) {
            if (entry.getRegion().equalsIgnoreCase(region)
                    && entry.getTableName().equalsIgnoreCase(tableName)) {
                return entry.getTableName();
            }
        }
        return tableName;
    }
}
