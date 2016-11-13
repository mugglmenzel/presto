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

import com.facebook.presto.dynamo.type.DynamoType;

/**
 * Created by menzelmi on 28/10/16.
 */
public class AwsDynamoToPrestoTypeMapper {

    public static DynamoType map(String awsDynamoType) {
        switch (awsDynamoType) {
            case "S":
                return DynamoType.STRING;
            case "LONG":
                return DynamoType.LONG;
            case "INT":
                return DynamoType.INT;
            case "N":
            case "DOUBLE":
                return DynamoType.DOUBLE;
            case "BOOL":
                return DynamoType.BOOLEAN;
            case "DATE":
                return DynamoType.DATE;
            case "DATETIME":
                return DynamoType.DATETIME;
            case "B":
                return DynamoType.BINARY;
            default:
                return DynamoType.STRING;
        }
    }
}
