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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.facebook.presto.dynamo.type.DynamoType;
import com.facebook.presto.dynamo.type.FullDynamoType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;

public class DynamoRecordCursor implements RecordCursor {
    private static final Logger Log = Logger.get(DynamoRecordCursor.class);

    private final AmazonDynamoDB dynamo;
    private final String tableName;
    private final Integer partitionId;
    private final Integer partitionCount;
    private final List<FullDynamoType> fullDynamoTypes;
    private final List<String> columnNames;
    private final int fetchSize;
    private final List<Map<String, AttributeValue>> rs = new ArrayList<Map<String, AttributeValue>>();

    private long currentRowIndexBase = 0;
    private long currentRowIndex = -1;
    private Map<String, AttributeValue> currentRow;

    private Map<String, AttributeValue> lastKeyEvaluated = null;

    public DynamoRecordCursor(AmazonDynamoDB dynamo, String tableName, Integer partitionId, Integer partitionCount,
                              List<FullDynamoType> fullDynamoTypes, List<String> columnNames,
                              int fetchSize) {
        this.dynamo = dynamo;

        this.tableName = tableName;
        this.partitionId = partitionId;
        this.partitionCount = partitionCount;
        this.fullDynamoTypes = fullDynamoTypes;
        this.columnNames = columnNames;
        this.fetchSize = fetchSize;

        currentRow = null;
    }

    @Override
    public boolean advanceNextPosition() {
        if (currentRowIndex == -1) {
            Log.info(String.format("Doing first scan for dynamo table %s",
                    tableName));
            ScanRequest scanRequest = new ScanRequest()
                    .withSegment(partitionId).withTotalSegments(partitionCount)
                    .withTableName(tableName).withLimit(fetchSize)
                    .withExclusiveStartKey(lastKeyEvaluated);
            ScanResult scanResult = dynamo.scan(scanRequest);
            List<Map<String, AttributeValue>> items = scanResult.getItems();
            rs.clear();
            rs.addAll(items);
            lastKeyEvaluated = scanResult.getLastEvaluatedKey();
            Log.info(String.format(
                    "Finished first scan for dynamo table %s, got %s rows",
                    tableName, items.size()));
            Log.info("Scanned items: " + items);
            if (items.size() == 0) {
                return false;
            } else {
                currentRowIndexBase = 0;
                currentRowIndex = currentRowIndexBase;
                currentRow = rs
                        .get((int) (currentRowIndex - currentRowIndexBase));
                return true;
            }
        } else if (currentRowIndex - currentRowIndexBase < rs.size() - 1) {
            currentRowIndex++;
            currentRow = rs.get((int) (currentRowIndex - currentRowIndexBase));
            return true;
        } else if (lastKeyEvaluated != null) {
            Log.info(String.format("Doing next scan for dynamo table %s",
                    tableName));
            ScanRequest scanRequest = new ScanRequest()
                    .withSegment(partitionId).withTotalSegments(partitionCount)
                    .withTableName(tableName).withLimit(fetchSize)
                    .withExclusiveStartKey(lastKeyEvaluated);
            ScanResult scanResult = dynamo.scan(scanRequest);
            List<Map<String, AttributeValue>> items = scanResult.getItems();
            rs.clear();
            rs.addAll(items);
            lastKeyEvaluated = scanResult.getLastEvaluatedKey();
            Log.info(String.format("Doing next scan for dynamo table %s",
                    tableName));

            if (items.size() == 0) {
                return false;
            } else {
                currentRowIndexBase = currentRowIndex + 1;
                currentRowIndex = currentRowIndexBase;
                currentRow = rs
                        .get((int) (currentRowIndex - currentRowIndexBase));
                return true;
            }
        } else {
            Log.info(String.format("Hit end for dynamo table %s", tableName));
            return false;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public boolean getBoolean(int i) {
        String columnName = columnNames.get(i);
        AttributeValue value = extractAttributeValue(columnName);
        return value.getBOOL();
    }

    @Override
    public long getCompletedBytes() {
        return currentRowIndex + 1;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        String columnName = columnNames.get(i);
        AttributeValue value = extractAttributeValue(columnName);
        return Double.parseDouble(value.getN());
    }

    @Override
    public long getLong(int i) {
        String columnName = columnNames.get(i);
        AttributeValue value = extractAttributeValue(columnName);
        return Long.parseLong(value.getN());
    }

    @Override
    public Object getObject(int i) {
        String columnName = columnNames.get(i);
        AttributeValue value = extractAttributeValue(columnName);
        return value.getS();
    }

    private AttributeValue extractAttributeValue(String columnName) {
        return currentRow.keySet().stream()
                .filter(k -> k.equalsIgnoreCase(columnName))
                .map(currentRow::get).findFirst()
                .orElse(currentRow.get(columnName));
    }

    private DynamoType getDynamoType(int i) {
        return fullDynamoTypes.get(i).getDynamoType();
    }

    @Override
    public Slice getSlice(int i) {
        String columnName = columnNames.get(i);
        Comparable<?> columnValue = DynamoType.getColumnValue(currentRow, columnName,
                fullDynamoTypes.get(i));
        if (columnValue == null) {
            return utf8Slice("");
        } else {
            return utf8Slice(columnValue.toString());
        }
    }

    @Override
    public long getTotalBytes() {
        return rs.size();
    }

    @Override
    public Type getType(int i) {
        return getDynamoType(i).getNativeType();
    }

    @Override
    public boolean isNull(int i) {
        String columnName = columnNames.get(i);
        AttributeValue attValue = extractAttributeValue(columnName);
        return attValue == null
                || (attValue.isNULL() != null && attValue.isNULL());
    }
}
