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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoColumnAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoTableAwsMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DynamoTestingUtils
{
    public static final String HOSTNAME = "localhost";
    public static final int PORT = 9142;
    public static final String REGION_AS_SCHEMA_NAME = "us_west_2";
    public static final String TABLE_NAME_Users = "Users";
    public static final String COLUMN_NAME_UserId = "UserId";
    public static final String COLUMN_NAME_UserName = "UserName";
    public static final String COLUMN_NAME_Age = "Age";
    public static final String TABLE_NAME_Books = "Books";
    public static final String COLUMN_NAME_BookName = "BookName";
    public static final String COLUMN_NAME_Writers = "Writers";
    private static final String CLUSTER_NAME = "TestCluster";

    private DynamoTestingUtils()
    {
    }

    public static String createTestMetadataFile()
    {
        File tempFile;
        try {
            tempFile = File.createTempFile("dynamo-metadata-test", ".tmp");
            tempFile.deleteOnExit();

            String metadataFilePath = tempFile.getAbsolutePath();

            DynamoAwsMetadata metadata = new DynamoAwsMetadata();
            List<DynamoColumnAwsMetadata> columns = new ArrayList<DynamoColumnAwsMetadata>();
            columns.add(new DynamoColumnAwsMetadata(
                    DynamoTestingUtils.COLUMN_NAME_UserId, DynamoType.STRING,
                    null));
            columns.add(new DynamoColumnAwsMetadata(
                    DynamoTestingUtils.COLUMN_NAME_UserName, DynamoType.STRING,
                    null));
            columns.add(new DynamoColumnAwsMetadata(
                    DynamoTestingUtils.COLUMN_NAME_Age, DynamoType.LONG, null));
            DynamoTableAwsMetadata table = new DynamoTableAwsMetadata(
                    Regions.US_WEST_2.toString().toLowerCase(), DynamoTestingUtils.TABLE_NAME_Users, columns);
            metadata.getTables().add(table);

            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(tempFile, metadata);

            return metadataFilePath;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
