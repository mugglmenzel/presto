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
package com.facebook.presto.dynamo.aws;

import com.amazonaws.regions.Regions;

public class AwsUtils
{
    private AwsUtils()
    {
    }

    public static Regions getRegionByEnumName(String enumName)
    {
        return valueOfIgnoreCase(Regions.class, enumName);
    }

    public static String getRegionAsSchemaName(String region)
    {
        return region.toLowerCase();
    }

    private static <T extends Enum<T>> T valueOfIgnoreCase(Class<T> enumType, String name)
    {
        for (T enumValue : enumType.getEnumConstants()) {
            if (enumValue.name().equalsIgnoreCase(name)) {
                return enumValue;
            }
        }

        throw new IllegalArgumentException(String.format("Invalid name %s for Enum %s", name, enumType.getName()));
    }
}
