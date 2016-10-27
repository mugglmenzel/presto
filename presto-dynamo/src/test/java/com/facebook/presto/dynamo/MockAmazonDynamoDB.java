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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.waiters.AmazonDynamoDBWaiters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockAmazonDynamoDB extends AbstractAmazonDynamoDB {
    public MockAmazonDynamoDB() {
    }

    @Override
    public void setEndpoint(String endpoint) throws IllegalArgumentException {
    }

    @Override
    public void setRegion(Region region) throws IllegalArgumentException {
    }

    @Override
    public ScanResult scan(ScanRequest scanRequest)
            throws AmazonServiceException, AmazonClientException {
        ScanResult scanResult = new ScanResult();

        List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();

        if (scanRequest.getTableName().equals(
                MockDynamoSession.TEST_TABLE)) {
            items.add(ImmutableMap.of(DynamoTestingUtils.COLUMN_NAME_UserId,
                    new AttributeValue("userId01"),
                    DynamoTestingUtils.COLUMN_NAME_UserName,
                    new AttributeValue("userName01")));
        } else if (scanRequest.getTableName().equals(
                DynamoTestingUtils.TABLE_NAME_Books)) {
            items.add(ImmutableMap.of(DynamoTestingUtils.COLUMN_NAME_BookName,
                    new AttributeValue("book01"),
                    DynamoTestingUtils.COLUMN_NAME_Writers, new AttributeValue(
                            ImmutableList.of("writer01", "writer02"))));
        }

        scanResult.setItems(items);

        return scanResult;
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public BatchWriteItemResult batchWriteItem(
            BatchWriteItemRequest batchWriteItemRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public DescribeTableResult describeTable(
            DescribeTableRequest describeTableRequest)
            throws AmazonServiceException, AmazonClientException {
        return new DescribeTableResult().withTable(new TableDescription().withItemCount(100000L));
    }

    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public QueryResult query(QueryRequest queryRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest listTablesRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public BatchGetItemResult batchGetItem(
            BatchGetItemRequest batchGetItemRequest)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ListTablesResult listTables() throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public ScanResult scan(String tableName, List<String> attributesToGet)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ScanResult scan(String tableName, Map<String, Condition> scanFilter)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ScanResult scan(String tableName, List<String> attributesToGet,
                           Map<String, Condition> scanFilter) throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public UpdateTableResult updateTable(String tableName,
                                         ProvisionedThroughput provisionedThroughput)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public DeleteTableResult deleteTable(String tableName)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
        return null;
    }

    @Override
    public BatchWriteItemResult batchWriteItem(
            Map<String, List<WriteRequest>> requestItems)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public GetItemResult getItem(String tableName,
                                 Map<String, AttributeValue> key) throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public GetItemResult getItem(String tableName,
                                 Map<String, AttributeValue> key, Boolean consistentRead)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public DeleteItemResult deleteItem(String tableName,
                                       Map<String, AttributeValue> key) throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public DeleteItemResult deleteItem(String tableName,
                                       Map<String, AttributeValue> key, String returnValues)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public CreateTableResult createTable(
            List<AttributeDefinition> attributeDefinitions, String tableName,
            List<KeySchemaElement> keySchema,
            ProvisionedThroughput provisionedThroughput)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public PutItemResult putItem(String tableName,
                                 Map<String, AttributeValue> item) throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public PutItemResult putItem(String tableName,
                                 Map<String, AttributeValue> item, String returnValues)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ListTablesResult listTables(String exclusiveStartTableName)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ListTablesResult listTables(String exclusiveStartTableName,
                                       Integer limit) throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public ListTablesResult listTables(Integer limit)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public UpdateItemResult updateItem(String tableName,
                                       Map<String, AttributeValue> key,
                                       Map<String, AttributeValueUpdate> attributeUpdates)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public UpdateItemResult updateItem(String tableName,
                                       Map<String, AttributeValue> key,
                                       Map<String, AttributeValueUpdate> attributeUpdates,
                                       String returnValues) throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public BatchGetItemResult batchGetItem(
            Map<String, KeysAndAttributes> requestItems,
            String returnConsumedCapacity) throws AmazonServiceException,
            AmazonClientException {
        return null;
    }

    @Override
    public BatchGetItemResult batchGetItem(
            Map<String, KeysAndAttributes> requestItems)
            throws AmazonServiceException, AmazonClientException {
        return null;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(
            AmazonWebServiceRequest request) {
        return null;
    }

    @Override
    public AmazonDynamoDBWaiters waiters() {
        return null;
    }
}
