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
package integrationtests;

import java.util.ArrayList;
import java.util.List;

import io.airlift.log.Logger;

import org.testng.annotations.Test;

import com.beust.jcommander.Strings;
import com.facebook.presto.dynamo.DynamoQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;

@Test(singleThreaded = true)
public class DynamoSqlQueryITCase extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(DynamoSqlQueryITCase.class);

    public DynamoSqlQueryITCase() throws Exception
    {
        super(DynamoQueryRunner.createDynamoQueryRunner());
    }

    @Test
    public void testQuery()
    {
        {
            MaterializedResult result = queryRunner.execute(
                    DynamoQueryRunner.createDynamoSession("us_west_2"),
                    "select * from us_west_2.users limit 10");
            printMaterializedResult(result);
        }
        {
            MaterializedResult result = queryRunner.execute(
                DynamoQueryRunner.createDynamoSession("US_WEST_2"),
                "select * from US_WEST_2.Users limit 10");
            printMaterializedResult(result);
        }
        {
            MaterializedResult result = queryRunner.execute(
                DynamoQueryRunner.createDynamoSession("us_west_2"),
                "select * from us_west_2.users limit 10");
            printMaterializedResult(result);
        }
    }

    private static void printMaterializedResult(MaterializedResult result)
    {
        int rowIndex = 0;
        for (MaterializedRow row : result) {
            log.info(String.format("---------- Row %s with %s fields---------", rowIndex++, row.getFieldCount()));
            List<String> list = new ArrayList<String>();
            for (int i = 0; i < row.getFieldCount(); i++) {
                Object obj = row.getField(i);
                list.add(String.valueOf(obj));
            }
            log.info("Field values: %s", String.join(", ", list));
        }
    }
}
