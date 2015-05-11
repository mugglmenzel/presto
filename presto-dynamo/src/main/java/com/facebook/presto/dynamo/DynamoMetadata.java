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

import static com.facebook.presto.dynamo.DynamoColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.dynamo.DynamoColumnHandle.columnMetadataGetter;
import static com.facebook.presto.dynamo.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.dynamo.aws.DynamoAwsMetadata;
import com.facebook.presto.dynamo.aws.DynamoAwsMetadataProvider;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class DynamoMetadata implements ConnectorMetadata
{
    private static final Logger log = Logger.get(DynamoMetadata.class);

    private final String connectorId;
    private final DynamoAwsMetadataProvider schemaProvider;
    private final DynamoSession dynamoSession;
    private final boolean allowDropTable;

    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;

    @Inject
    public DynamoMetadata(DynamoConnectorId connectorId,
            DynamoAwsMetadataProvider schemaProvider,
            DynamoSession dynamoSession,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            DynamoClientConfig config)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null")
                .toString();
        this.schemaProvider = checkNotNull(schemaProvider,
                "schemaProvider is null");
        this.dynamoSession = checkNotNull(dynamoSession,
                "dynamoSession is null");
        this.allowDropTable = checkNotNull(config, "config is null")
                .getAllowDropTable();
        this.extraColumnMetadataCodec = checkNotNull(extraColumnMetadataCodec,
                "extraColumnMetadataCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return schemaProvider.getMetadata().getRegionsAsSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
            SchemaTableName tableName)
    {
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
        }
        catch (IllegalArgumentException e) { // Enum exception
            log.debug(e, "Failed to get handle for table: " + tableName);
            return null;
        }
        catch (NotFoundException e) {
            log.debug(e, "Failed to get handle for table: " + tableName);
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, DynamoTableHandle.class, "tableHandle")
                .getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        DynamoTableHandle tableHandle = new DynamoTableHandle(connectorId,
                tableName.getSchemaName(), tableName.getTableName());
        List<ColumnMetadata> columns = ImmutableList
                .copyOf(transform(getColumnHandles(tableHandle).values(),
                        columnMetadataGetter()));
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session,
            String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList
                .builder();
        DynamoAwsMetadata metdata = schemaProvider.getMetadata();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : metdata.getTableNames(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase()));
                }
            }
            catch (IllegalArgumentException e) {
                // Enum exception
            }
            catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session,
            String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(
            ConnectorTableHandle tableHandle)
    {
        return getColumnHandles(tableHandle, true).get(
                SAMPLE_WEIGHT_COLUMN_NAME);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorTableHandle tableHandle)
    {
        return getColumnHandles(tableHandle, false);
    }

    private Map<String, ColumnHandle> getColumnHandles(
            ConnectorTableHandle tableHandle, boolean includeSampleWeight)
    {
        DynamoTableHandle dynamoTableHandle = (DynamoTableHandle) tableHandle;
        DynamoAwsMetadata metadata = schemaProvider.getMetadata();
        DynamoTable table = DynamoTable.getTable(metadata, connectorId,
                dynamoTableHandle.getSchemaName(),
                dynamoTableHandle.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap
                .builder();
        for (DynamoColumnHandle columnHandle : table.getColumns()) {
            if (includeSampleWeight
                    || !columnHandle.getName()
                            .equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columnHandles.put(columnHandle.getName(),
                        columnHandle);
            }
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap
                .builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(),
                prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        checkType(tableHandle, DynamoTableHandle.class, "tableHandle");
        return checkType(columnHandle, DynamoColumnHandle.class, "columnHandle")
                .getColumnMetadata();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("connectorId", connectorId).toString();
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public void createTable(ConnectorSession session,
            ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "CREATE TABLE not yet supported for Dynamo");
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "Dropping tables not yet supported for Dynamo");
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle,
            SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "Renaming tables not yet supported for Dynamo");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "Creating tables not yet supported for Dynamo");
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "Createing tables not yet supported for Dynamo");
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "INSERT not yet supported for Dynamo");
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName,
            String viewData, boolean replace)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "CREATE VIEW not yet supported for Dynamo");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "DROP VIEW not yet supported for Dynamo");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session,
            String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        return emptyMap();
    }
}
