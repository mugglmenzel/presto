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

import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.metadata.DynamoColumnAwsMetadata;
import com.facebook.presto.dynamo.aws.metadata.DynamoTableAwsMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.dynamo.DynamoColumnHandle.hashKeyPredicate;
import static com.google.common.base.MoreObjects.toStringHelper;

public class DynamoTable {
    private final DynamoTableHandle tableHandle;
    private final List<DynamoColumnHandle> columns;

    public DynamoTable(DynamoTableHandle tableHandle,
                       List<DynamoColumnHandle> columns) {
        this.tableHandle = tableHandle;
        this.columns = ImmutableList.copyOf(columns);
    }

    public List<DynamoColumnHandle> getColumns() {
        return columns;
    }

    public DynamoTableHandle getTableHandle() {
        return tableHandle;
    }

    public List<DynamoColumnHandle> getPartitionKeyColumns() {
        return ImmutableList.copyOf(Iterables.filter(columns,
                hashKeyPredicate()));
    }

    public String getTokenExpression() {
        StringBuilder sb = new StringBuilder();
        for (DynamoColumnHandle column : getPartitionKeyColumns()) {
            if (sb.length() == 0) {
                sb.append("token(");
            } else {
                sb.append(",");
            }
            sb.append(column.getName());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DynamoTable)) {
            return false;
        }
        DynamoTable that = (DynamoTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("tableHandle", tableHandle).toString();
    }

    public static DynamoTable getTable(DynamoAwsMetadata metadata,
                                       String connectorId, String schemaName, String tableName)
            throws TableNotFoundException {
        DynamoTableHandle tableHandle = new DynamoTableHandle(connectorId,
                schemaName, tableName);

        DynamoTableAwsMetadata metadataTable = null;
        for (DynamoTableAwsMetadata entry : metadata.getTables()) {
            if (entry.getRegion().equalsIgnoreCase(schemaName)
                    && entry.getTableName().equalsIgnoreCase(tableName)) {
                metadataTable = entry;
                break;
            }
        }

        if (metadataTable == null) {
            throw new TableNotFoundException(new SchemaTableName(schemaName,
                    tableName), "Not found in metadata");
        }

        List<DynamoColumnHandle> columnHandles = new ArrayList<DynamoColumnHandle>();
        int pos = 0;
        for (DynamoColumnAwsMetadata entry : metadataTable.getColumns()) {
            DynamoColumnHandle columnHandler = new DynamoColumnHandle(
                    connectorId, entry.getColumnName(), pos++,
                    entry.getColumnType(), entry.getTypeArguments(), false,
                    false, false, false);
            columnHandles.add(columnHandler);
        }

        return new DynamoTable(tableHandle, columnHandles);
    }
}
