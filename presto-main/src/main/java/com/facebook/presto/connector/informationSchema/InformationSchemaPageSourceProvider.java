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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.PartitionResult;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.split.SplitManager;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_VIEWS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.informationSchemaTableColumns;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Sets.union;
import static java.lang.String.format;

public class InformationSchemaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Metadata metadata;
    private final SplitManager splitManager;

    @Inject
    public InformationSchemaPageSourceProvider(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        InternalTable table = getInternalTable(split, columns);

        List<Integer> channels = new ArrayList<>();
        for (ConnectorColumnHandle column : columns) {
            String columnName = checkType(column, InformationSchemaColumnHandle.class, "column").getColumnName();
            int columnIndex = table.getColumnIndex(columnName);
            channels.add(columnIndex);
        }

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (Page page : table.getPages()) {
            Block[] blocks = new Block[channels.size()];
            for (int index = 0; index < blocks.length; index++) {
                blocks[index] = page.getBlock(channels.get(index));
            }
            pages.add(new Page(page.getPositionCount(), blocks));
        }
        return new FixedPageSource(pages.build());
    }

    private InternalTable getInternalTable(ConnectorSplit connectorSplit, List<ConnectorColumnHandle> columns)
    {
        InformationSchemaSplit split = checkType(connectorSplit, InformationSchemaSplit.class, "split");

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        InformationSchemaTableHandle handle = split.getTableHandle();
        Map<String, Object> filters = split.getFilters();

        return getInformationSchemaTable(handle.getSession(), handle.getCatalogName(), handle.getSchemaTableName(), filters);
    }

    public InternalTable getInformationSchemaTable(ConnectorSession session, String catalog, SchemaTableName table, Map<String, Object> filters)
    {
        if (table.equals(TABLE_COLUMNS)) {
            return buildColumns(session, catalog, filters);
        }
        if (table.equals(TABLE_TABLES)) {
            return buildTables(session, catalog, filters);
        }
        if (table.equals(TABLE_VIEWS)) {
            return buildViews(session, catalog, filters);
        }
        if (table.equals(TABLE_SCHEMATA)) {
            return buildSchemata(session, catalog);
        }
        if (table.equals(TABLE_INTERNAL_FUNCTIONS)) {
            return buildFunctions();
        }
        if (table.equals(TABLE_INTERNAL_PARTITIONS)) {
            return buildPartitions(session, catalog, filters);
        }

        throw new IllegalArgumentException(format("table does not exist: %s", table));
    }

    private InternalTable buildColumns(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_COLUMNS));
        for (Entry<QualifiedTableName, List<ColumnMetadata>> entry : getColumnsList(session, catalogName, filters).entrySet()) {
            QualifiedTableName tableName = entry.getKey();
            for (ColumnMetadata column : entry.getValue()) {
                if (column.isHidden()) {
                    continue;
                }
                table.add(
                        tableName.getCatalogName(),
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        column.getName(),
                        column.getOrdinalPosition() + 1,
                        null,
                        "YES",
                        column.getType().getName(),
                        column.isPartitionKey() ? "YES" : "NO",
                        column.getComment());
            }
        }
        return table.build();
    }

    private Map<QualifiedTableName, List<ColumnMetadata>> getColumnsList(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        return metadata.listTableColumns(session, extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildTables(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        Set<QualifiedTableName> tables = ImmutableSet.copyOf(getTablesList(session, catalogName, filters));
        Set<QualifiedTableName> views = ImmutableSet.copyOf(getViewsList(session, catalogName, filters));

        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_TABLES));
        for (QualifiedTableName name : union(tables, views)) {
            // if table and view names overlap, the view wins
            String type = views.contains(name) ? "VIEW" : "BASE TABLE";
            table.add(
                    name.getCatalogName(),
                    name.getSchemaName(),
                    name.getTableName(),
                    type);
        }
        return table.build();
    }

    private List<QualifiedTableName> getTablesList(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        return metadata.listTables(session, extractQualifiedTablePrefix(catalogName, filters));
    }

    private List<QualifiedTableName> getViewsList(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        return metadata.listViews(session, extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildViews(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_VIEWS));
        for (Entry<QualifiedTableName, ViewDefinition> entry : getViews(session, catalogName, filters).entrySet()) {
            table.add(
                    entry.getKey().getCatalogName(),
                    entry.getKey().getSchemaName(),
                    entry.getKey().getTableName(),
                    entry.getValue().getOriginalSql());
        }
        return table.build();
    }

    private Map<QualifiedTableName, ViewDefinition> getViews(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        return metadata.getViews(session, extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildFunctions()
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_INTERNAL_FUNCTIONS));
        for (ParametricFunction function : metadata.listFunctions()) {
            if (function.isApproximate()) {
                continue;
            }

            String functionType;
            if (function.isAggregate()) {
                functionType = "aggregate";
            }
            else if (function.isWindow()) {
                functionType = "window";
            }
            else if (function.isDeterministic()) {
                functionType = "scalar";
            }
            else {
                functionType = "scalar (non-deterministic)";
            }

            table.add(
                    function.getSignature().getName(),
                    Joiner.on(", ").join(function.getSignature().getArgumentTypes()),
                    function.getSignature().getReturnType(),
                    functionType,
                    nullToEmpty(function.getDescription()));
        }
        return table.build();
    }

    private InternalTable buildSchemata(ConnectorSession session, String catalogName)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_SCHEMATA));
        for (String schema : metadata.listSchemaNames(session, catalogName)) {
            table.add(catalogName, schema);
        }
        return table.build();
    }

    private InternalTable buildPartitions(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        QualifiedTableName tableName = extractQualifiedTableName(catalogName, filters);

        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_INTERNAL_PARTITIONS));
        int partitionNumber = 1;

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        checkArgument(tableHandle.isPresent(), "Table %s does not exist", tableName);
        Map<ColumnHandle, String> columnHandles = ImmutableBiMap.copyOf(metadata.getColumnHandles(tableHandle.get())).inverse();
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle.get(), Optional.<TupleDomain<ColumnHandle>>absent());

        for (Partition partition : partitionResult.getPartitions()) {
            for (Entry<ColumnHandle, Comparable<?>> entry : partition.getTupleDomain().extractFixedValues().entrySet()) {
                ColumnHandle columnHandle = entry.getKey();
                String columnName = columnHandles.get(columnHandle);
                String value = null;
                if (entry.getValue() != null) {
                    ColumnMetadata columnMetadata  = metadata.getColumnMetadata(tableHandle.get(), columnHandle);
                    try {
                        FunctionInfo operator = metadata.getFunctionRegistry().getCoercion(columnMetadata.getType(), VARCHAR);
                        value = ((Slice) operator.getMethodHandle().invokeWithArguments(entry.getValue())).toStringUtf8();
                    }
                    catch (OperatorNotFoundException e) {
                        value = "<UNREPRESENTABLE VALUE>";
                    }
                    catch (Throwable throwable) {
                        throw Throwables.propagate(throwable);
                    }
                }
                table.add(
                        catalogName,
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        partitionNumber,
                        columnName,
                        value);
            }
            partitionNumber++;
        }
        return table.build();
    }

    private static QualifiedTableName extractQualifiedTableName(String catalogName, Map<String, Object> filters)
    {
        Optional<String> schemaName = getFilterColumn(filters, "table_schema");
        checkArgument(schemaName.isPresent(), "filter is required for column: %s.%s", TABLE_INTERNAL_PARTITIONS, "table_schema");
        Optional<String> tableName = getFilterColumn(filters, "table_name");
        checkArgument(tableName.isPresent(), "filter is required for column: %s.%s", TABLE_INTERNAL_PARTITIONS, "table_name");
        return new QualifiedTableName(catalogName, schemaName.get(), tableName.get());
    }

    private static QualifiedTablePrefix extractQualifiedTablePrefix(String catalogName, Map<String, Object> filters)
    {
        Optional<String> schemaName = getFilterColumn(filters, "table_schema");
        Optional<String> tableName = getFilterColumn(filters, "table_name");
        if (!schemaName.isPresent()) {
            return new QualifiedTablePrefix(catalogName, Optional.<String>absent(), Optional.<String>absent());
        }
        return new QualifiedTablePrefix(catalogName, schemaName, tableName);
    }

    private static Optional<String> getFilterColumn(Map<String, Object> filters, String columnName)
    {
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            if (entry.getKey().equals(columnName)) {
                if (entry.getValue() instanceof String) {
                    return Optional.of((String) entry.getValue());
                }
                if (entry.getValue() instanceof Slice) {
                    return Optional.of(((Slice) entry.getValue()).toStringUtf8());
                }
                break;
            }
        }
        return Optional.absent();
    }
}
