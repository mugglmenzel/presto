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
import com.facebook.presto.dynamo.aws.metadata.DynamoAwsMetadataProvider;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.dynamo.DynamoColumnHandle.columnMetadataGetter;
import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class DynamoMetadata implements ConnectorMetadata {
    private static final Logger log = Logger.get(DynamoMetadata.class);

    private final String connectorId;
    private final DynamoAwsMetadataProvider schemaProvider;
    private final DynamoSession dynamoSession;
    private final DynamoSplitManager splitManager;
    private final boolean allowDropTable;

    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;

    @Inject
    public DynamoMetadata(DynamoConnectorId connectorId,
                          DynamoAwsMetadataProvider schemaProvider,
                          DynamoSession dynamoSession,
                          DynamoSplitManager splitManager,
                          JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
                          DynamoClientConfig config) {
        this.connectorId = checkNotNull(connectorId, "connectorId is null")
                .toString();
        this.schemaProvider = checkNotNull(schemaProvider,
                "schemaProvider is null");
        this.dynamoSession = checkNotNull(dynamoSession,
                "dynamoSession is null");
        this.splitManager = checkNotNull(splitManager,
                "splitManager is null");
        this.allowDropTable = checkNotNull(config, "config is null")
                .getAllowDropTable();
        this.extraColumnMetadataCodec = checkNotNull(extraColumnMetadataCodec,
                "extraColumnMetadataCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return schemaProvider.getMetadata().getRegionsAsSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
                                               SchemaTableName tableName) {
        checkNotNull(tableName, "tableName is null");
        try {
            if (schemaProvider.getMetadata().getTable(
                    tableName.getSchemaName(), tableName.getTableName()) == null) {
                log.info("Meta does not contain table: " + tableName);
                return null;
            }
            DynamoTableHandle tableHandle = new DynamoTableHandle(connectorId,
                    tableName.getSchemaName(), tableName.getTableName());
            return tableHandle;
        } catch (IllegalArgumentException e) { // Enum exception
            log.debug(e, "Failed to get handle for table: " + tableName);
            return null;
        } catch (NotFoundException e) {
            log.debug(e, "Failed to get handle for table: " + tableName);
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle) {
        return checkType(tableHandle, DynamoTableHandle.class, "tableHandle")
                .getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                   ConnectorTableHandle tableHandle) {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(session, tableName);
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName) {
        DynamoTableHandle tableHandle = new DynamoTableHandle(connectorId,
                tableName.getSchemaName(), tableName.getTableName());
        List<ColumnMetadata> columns = ImmutableList
                .copyOf(transform(getColumnHandles(session, tableHandle).values(),
                        columnMetadataGetter()));
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session,
                                            String schemaNameOrNull) {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList
                .builder();
        DynamoAwsMetadata metdata = schemaProvider.getMetadata();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : metdata.getTableNames(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase()));
                }
            } catch (IllegalArgumentException e) {
                // Enum exception
            } catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session,
                                     String schemaNameOrNull) {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle tableHandle) {
        DynamoTableHandle dynamoTableHandle = (DynamoTableHandle) tableHandle;
        DynamoAwsMetadata metadata = schemaProvider.getMetadata();
        DynamoTable table = DynamoTable.getTable(metadata, connectorId,
                dynamoTableHandle.getSchemaName(),
                dynamoTableHandle.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap
                .builder();
        log.debug("Metadata columns: " + table.getColumns());
        for (DynamoColumnHandle columnHandle : table.getColumns())
            columnHandles.put(columnHandle.getName(),
                    columnHandle);
        log.debug("Retrieved columnhandles: " + columnHandles.build());
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix) {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap
                .builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            } catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session,
                                             SchemaTablePrefix prefix) {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(),
                prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        checkType(tableHandle, DynamoTableHandle.class, "tableHandle");
        return checkType(columnHandle, DynamoColumnHandle.class, "columnHandle")
                .getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        DynamoTableHandle handle = checkType(table, DynamoTableHandle.class, "table");

        ConnectorTableLayout layout = getTableLayout(session, new DynamoTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("connectorId", connectorId).toString();
    }

}
