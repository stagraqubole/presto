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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.HadoopFileSystemCache;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;
import com.facebook.presto.hive.shaded.org.apache.thrift.TException;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveType.columnTypeToHiveType;
import static com.facebook.presto.hive.HiveType.getHiveType;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveType.hiveTypeNameGetter;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.getTableStructFields;
import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static com.facebook.presto.hive.HiveUtil.partitionIdGetter;
import static com.facebook.presto.hive.UnpartitionedPartition.UNPARTITIONED_PARTITION;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;
import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;
import static org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import static com.facebook.presto.hive.HiveFSUtils.pathExists;
import static com.facebook.presto.hive.HiveFSUtils.createDirectories;
import static com.facebook.presto.hive.HiveFSUtils.isDirectory;
import static com.facebook.presto.hive.HiveFSUtils.rename;
import static com.facebook.presto.hive.HiveFSUtils.getPartitionValues;
import static com.facebook.presto.hive.HiveFSUtils.delete;

@SuppressWarnings("deprecation")
public class HiveClient
        implements ConnectorMetadata, ConnectorSplitManager, ConnectorRecordSinkProvider, ConnectorHandleResolver
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    public static final String PRESTO_OFFLINE = "presto_offline";

    private static final Logger log = Logger.get(HiveClient.class);
    private static final int partionCommitBatchSize = 8;
    private static final int renameThreadPoolSize = 20;

    private final String connectorId;
    private final int maxOutstandingSplits;
    private final int maxSplitIteratorThreads;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowCorruptWritesForTesting;
    private final HiveMetastore metastore;
    private final NamenodeStats namenodeStats;
    private final HdfsEnvironment hdfsEnvironment;
    private final DirectoryLister directoryLister;
    private final DateTimeZone timeZone;
    private final Executor executor;
    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final int maxInitialSplits;
    private final HiveStorageFormat hiveStorageFormat;
    private final boolean recursiveDfsWalkerEnabled;
    private final TypeManager typeManager;
    private final boolean insertS3TempEnabled;

    @Inject
    public HiveClient(HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager)
    {
        this(connectorId,
                metastore,
                namenodeStats,
                hdfsEnvironment,
                directoryLister,
                DateTimeZone.forTimeZone(hiveClientConfig.getTimeZone()),
                new BoundedExecutor(executorService, hiveClientConfig.getMaxGlobalSplitIteratorThreads()),
                hiveClientConfig.getMaxSplitSize(),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.getMaxSplitIteratorThreads(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplitSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.getAllowDropTable(),
                hiveClientConfig.getAllowRenameTable(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                hiveClientConfig.getHiveStorageFormat(),
                hiveClientConfig.getInsertS3TempEnabled(),
                false,
                typeManager);
    }

    public HiveClient(HiveConnectorId connectorId,
            HiveMetastore metastore,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            DateTimeZone timeZone,
            Executor executor,
            DataSize maxSplitSize,
            int maxOutstandingSplits,
            int maxSplitIteratorThreads,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            DataSize maxInitialSplitSize,
            int maxInitialSplits,
            boolean allowDropTable,
            boolean allowRenameTable,
            boolean allowCorruptWritesForTesting,
            HiveStorageFormat hiveStorageFormat,
            boolean insertS3TempEnabled,
            boolean recursiveDfsWalkerEnabled,
            TypeManager typeManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        this.maxSplitSize = checkNotNull(maxSplitSize, "maxSplitSize is null");
        checkArgument(maxOutstandingSplits > 0, "maxOutstandingSplits must be at least 1");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxSplitIteratorThreads = maxSplitIteratorThreads;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxInitialSplitSize = checkNotNull(maxInitialSplitSize, "maxInitialSplitSize is null");
        this.maxInitialSplits = maxInitialSplits;
        this.allowDropTable = allowDropTable;
        this.allowRenameTable = allowRenameTable;
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;

        this.metastore = checkNotNull(metastore, "metastore is null");
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.namenodeStats = checkNotNull(namenodeStats, "namenodeStats is null");
        this.directoryLister = checkNotNull(directoryLister, "directoryLister is null");
        this.timeZone = checkNotNull(timeZone, "timeZone is null");

        this.executor = checkNotNull(executor, "executor is null");

        this.recursiveDfsWalkerEnabled = recursiveDfsWalkerEnabled;
        this.hiveStorageFormat = hiveStorageFormat;
        this.typeManager = checkNotNull(typeManager, "typeManager is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }
        HiveFSUtils.initialize(hdfsEnvironment.getConfiguration());
        this.insertS3TempEnabled = insertS3TempEnabled;
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        try {
            metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            return new HiveTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName(), session);
        }
        catch (NoSuchObjectException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, HiveTableHandle.class, "tableHandle").getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
                throw new TableNotFoundException(tableName);
            }
            List<ColumnMetadata> columns = ImmutableList.copyOf(transform(getColumnHandles(table, false), columnMetadataGetter(table, typeManager)));
            return new ConnectorTableMetadata(tableName, columns, table.getOwner());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : metastore.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (NoSuchObjectException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            for (HiveColumnHandle columnHandle : getColumnHandles(table, true)) {
                if (columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                    return columnHandle;
                }
            }
            return null;
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
            for (HiveColumnHandle columnHandle : getColumnHandles(table, false)) {
                columnHandles.put(columnHandle.getName(), columnHandle);
            }
            return columnHandles.build();
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    private List<HiveColumnHandle> getColumnHandles(Table table, boolean includeSampleWeight)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        // add the data fields first
        int hiveColumnIndex = 0;
        for (StructField field : getTableStructFields(table)) {
            // ignore unsupported types rather than failing
            HiveType hiveType = getHiveType(field.getFieldObjectInspector());
            if (hiveType != null && (includeSampleWeight || !field.getFieldName().equals(SAMPLE_WEIGHT_COLUMN_NAME))) {
                columns.add(new HiveColumnHandle(connectorId, field.getFieldName(), hiveColumnIndex, hiveType, getType(field.getFieldObjectInspector()).getName(), hiveColumnIndex, false));
            }
            hiveColumnIndex++;
        }

        // add the partition keys last (like Hive does)
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);

            HiveType hiveType = getSupportedHiveType(field.getType());
            columns.add(new HiveColumnHandle(connectorId, field.getName(), hiveColumnIndex + i, hiveType, getType(field.getType()).getName(), -1, true));
        }

        return columns.build();
    }

    public static Type getType(String hiveType)
    {
        switch (hiveType) {
            case BOOLEAN_TYPE_NAME:
                return BOOLEAN;
            case TINYINT_TYPE_NAME:
                return BIGINT;
            case SMALLINT_TYPE_NAME:
                return BIGINT;
            case INT_TYPE_NAME:
                return BIGINT;
            case BIGINT_TYPE_NAME:
                return BIGINT;
            case FLOAT_TYPE_NAME:
                return DOUBLE;
            case DOUBLE_TYPE_NAME:
                return DOUBLE;
            case STRING_TYPE_NAME:
                return VARCHAR;
            case TIMESTAMP_TYPE_NAME:
                return TIMESTAMP;
            case BINARY_TYPE_NAME:
                return VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported hive type " + hiveType);
        }
    }

    public static Type getType(ObjectInspector fieldInspector)
    {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory();
                return getPrimitiveType(primitiveCategory);
            case MAP:
                return VARCHAR;
            case LIST:
                return VARCHAR;
            case STRUCT:
                return VARCHAR;
            default:
                throw new IllegalArgumentException("Unsupported hive type " + fieldInspector.getTypeName());
        }
    }

    private static Type getPrimitiveType(PrimitiveCategory primitiveCategory)
    {
        switch (primitiveCategory) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return BIGINT;
            case SHORT:
                return BIGINT;
            case INT:
                return BIGINT;
            case LONG:
                return BIGINT;
            case FLOAT:
                return DOUBLE;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return VARCHAR;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return VARBINARY;
            default:
                return null;
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        return checkType(columnHandle, HiveColumnHandle.class, "columnHandle").getColumnMetadata(typeManager);
    }

    @Override
    public ConnectorTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            throw new PrestoException(PERMISSION_DENIED.toErrorCode(), "Renaming tables is disabled in this Hive catalog");
        }

        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        SchemaTableName tableName = getTableName(tableHandle);

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED.toErrorCode(), "DROP TABLE is disabled in this Hive catalog");
        }

        try {
            Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
            if (!handle.getSession().getUser().equals(table.getOwner())) {
                throw new PrestoException(PERMISSION_DENIED.toErrorCode(), format("Unable to drop table '%s': owner of the table is different from session user", table));
            }
            metastore.dropTable(handle.getSchemaName(), handle.getTableName());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(allowCorruptWritesForTesting || timeZone.equals(DateTimeZone.getDefault()),
                "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments",
                timeZone.getID());

        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        if (tableMetadata.isSampled()) {
            columnNames.add(SAMPLE_WEIGHT_COLUMN_NAME);
            columnTypes.add(BIGINT);
        }

        // get the root directory for the database
        SchemaTableName table = tableMetadata.getTable();
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();

        String location = getDatabase(schemaName).getLocationUri();
        if (isNullOrEmpty(location)) {
            throw new RuntimeException(format("Database '%s' location is not set", schemaName));
        }

        Path databasePath = new Path(location);
        if (!pathExists(databasePath)) {
            throw new RuntimeException(format("Database '%s' location does not exist: %s", schemaName, databasePath));
        }
        if (!isDirectory(databasePath)) {
            throw new RuntimeException(format("Database '%s' location is not a directory: %s", schemaName, databasePath));
        }

        // verify the target directory for the table
        Path targetPath = new Path(databasePath, tableName);
        if (pathExists(targetPath)) {
            throw new RuntimeException(format("Target directory for table '%s' already exists: %s", table, targetPath));
        }

        if (!useTemporaryDirectory(targetPath)) {
            return new HiveOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build(),
                tableMetadata.getOwner(),
                targetPath.toString(),
                targetPath.toString());
        }

        return new HiveOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build(),
                tableMetadata.getOwner(),
                targetPath.toString(),
                createTemporaryPath(targetPath));
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<String> fragments)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        // verify no one raced us to create the target directory
        Path targetPath = new Path(handle.getTargetPath());

        // rename if using a temporary directory
        if (handle.hasTemporaryPath()) {
            if (pathExists(targetPath)) {
                SchemaTableName table = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
                throw new RuntimeException(format("Unable to commit creation of table '%s': target directory already exists: %s", table, targetPath));
            }
            // rename the temporary directory to the target
            rename(new Path(handle.getTemporaryPath()), targetPath);
        }

        // create the table in the metastore
        List<String> types = FluentIterable.from(handle.getColumnTypes())
                .transform(columnTypeToHiveType())
                .transform(hiveTypeNameGetter())
                .toList();

        boolean sampled = false;
        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for (int i = 0; i < handle.getColumnNames().size(); i++) {
            String name = handle.getColumnNames().get(i);
            String type = types.get(i);
            if (name.equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columns.add(new FieldSchema(name, type, "Presto sample weight column"));
                sampled = true;
            }
            else {
                columns.add(new FieldSchema(name, type, null));
            }
        }

        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(handle.getTableName());
        serdeInfo.setSerializationLib(hiveStorageFormat.getSerDe());
        serdeInfo.setParameters(ImmutableMap.<String, String>of());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(targetPath.toString());
        sd.setCols(columns.build());
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(hiveStorageFormat.getInputFormat());
        sd.setOutputFormat(hiveStorageFormat.getOutputFormat());
        sd.setParameters(ImmutableMap.<String, String>of());

        Table table = new Table();
        table.setDbName(handle.getSchemaName());
        table.setTableName(handle.getTableName());
        table.setOwner(handle.getTableOwner());
        table.setTableType(TableType.MANAGED_TABLE.toString());
        String tableComment = "Created by Presto";
        if (sampled) {
            tableComment = "Sampled table created by Presto. Only query this table from Hive if you understand how Presto implements sampling.";
        }
        table.setParameters(ImmutableMap.of("comment", tableComment));
        table.setPartitionKeys(ImmutableList.<FieldSchema>of());
        table.setSd(sd);

        metastore.createTable(table);
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        Path target = new Path(handle.getTemporaryPath());
        JobConf conf = new JobConf(hdfsEnvironment.getConfiguration(target));

        return new HiveRecordSink(handle, target, conf);
    }

    @Override
    public RecordSink getRecordSink(ConnectorInsertTableHandle tableHandle)
    {
        HiveInsertTableHandle handle = checkType(tableHandle, HiveInsertTableHandle.class, "tableHandle");

        Path target = new Path(handle.getTemporaryPath());
        JobConf conf = new JobConf(hdfsEnvironment.getConfiguration(target));

        return new HiveRecordSink(handle, target, conf);
    }

    private Database getDatabase(String database)
    {
        try {
            return metastore.getDatabase(database);
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(database);
        }
    }

    private boolean useTemporaryDirectory(Path path)
    {
        try {
            // skip using temporary directory for S3
            return !(hdfsEnvironment.getFileSystem(path) instanceof PrestoS3FileSystem);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        if (replace) {
            try {
                dropView(session, viewName);
            }
            catch (ViewNotFoundException ignored) {
            }
        }

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("comment", "Presto View")
                .put(PRESTO_VIEW_FLAG, "true")
                .build();

        FieldSchema dummyColumn = new FieldSchema("dummy", STRING_TYPE_NAME, null);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(ImmutableList.of(dummyColumn));
        sd.setSerdeInfo(new SerDeInfo());

        Table table = new Table();
        table.setDbName(viewName.getSchemaName());
        table.setTableName(viewName.getTableName());
        table.setOwner(session.getUser());
        table.setTableType(TableType.VIRTUAL_VIEW.name());
        table.setParameters(properties);
        table.setViewOriginalText(encodeViewData(viewData));
        table.setViewExpandedText("/* Presto View */");
        table.setSd(sd);

        try {
            metastore.createTable(table);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        String view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
        if (view == null) {
            throw new ViewNotFoundException(viewName);
        }

        try {
            metastore.dropTable(viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : metastore.getAllViews(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (NoSuchObjectException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, String> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        for (SchemaTableName schemaTableName : tableNames) {
            try {
                Table table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
                if (HiveUtil.isPrestoView(table)) {
                    views.put(schemaTableName, decodeViewData(table.getViewOriginalText()));
                }
            }
            catch (NoSuchObjectException ignored) {
            }
        }

        return views.build();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(allowCorruptWritesForTesting || timeZone.equals(DateTimeZone.getDefault()),
                "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments",
                timeZone.getID());
        List<Boolean> partitionBitmap = null;

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        // call metastore to get table location, outputFormat, serde, partitions indices
        Table table = null;
        SchemaTableName tableSchemaName = getTableName(tableHandle);
        try {
            table = metastore.getTable(tableSchemaName.getSchemaName(), tableSchemaName.getTableName());
        }
        catch (NoSuchObjectException e) {
            table = null;
        }

        checkNotNull(table, "Table %s does not exist", tableSchemaName.getTableName());
        if (table.getSd().getNumBuckets() > 0) {
            throw new UnsupportedOperationException("Insert not supported with Bucketed Tables");
        }

        String outputFormat = table.getSd().getOutputFormat();
        SerDeInfo serdeInfo = table.getSd().getSerdeInfo();
        String serdeLib = serdeInfo.getSerializationLib();
        Map<String, String> serdeParameters = serdeInfo.getParameters();

        String location = table.getSd().getLocation();
        ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);

        if (table.getPartitionKeysSize() != 0) {
            partitionBitmap = new ArrayList<Boolean>(tableMetadata.getColumns().size());

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                partitionBitmap.add(column.isPartitionKey());
            }
        }

        if (tableMetadata.isSampled()) {
            columnNames.add(SAMPLE_WEIGHT_COLUMN_NAME);
            columnTypes.add(BIGINT);
        }
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }

        return buildInsert(
                tableSchemaName.getSchemaName(),
                tableSchemaName.getTableName(),
                location,
                columnNames.build(),
                columnTypes.build(),
                outputFormat,
                serdeLib,
                serdeParameters,
                partitionBitmap);
    }

    private ConnectorInsertTableHandle buildInsert(String schemaName,
            String tableName,
            String location,
            List<String> columnNames,
            List<Type> columnTypes,
            String outputFormat,
            String serdeLib,
            Map<String, String> serdeParameters,
            List<Boolean> partitionBitmap)
    {
        Path targetPath = new Path(location);
        if (!pathExists(targetPath)) {
            createDirectories(targetPath);
        }

        String tempPath;
        String filePrefix = randomUUID().toString();

        log.info(String.format("Using '%s' as file prefix for insert", filePrefix));

        if ((useTemporaryDirectory(targetPath) || insertS3TempEnabled)) {
            tempPath = createTemporaryPath(targetPath);
        }
        else {
            tempPath = targetPath.toString();
        }

        return new HiveInsertTableHandle(
                    connectorId,
                    schemaName,
                    tableName,
                    columnNames,
                    columnTypes,
                    targetPath.toString(),
                    tempPath,
                    outputFormat,
                    serdeLib,
                    serdeParameters,
                    partitionBitmap,
                    filePrefix);
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<String> fragments)
    {
        HiveInsertTableHandle handle = checkType(insertHandle, HiveInsertTableHandle.class, "invalid insertHandle");
        Map<String, List<String>> filesWritten = new HashMap<String, List<String>>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            for (String fragment : fragments) {
                if (fragment.length() == 0) {
                    log.warn("Empty fragment for Insert on table " + handle.getTableName());
                    continue;
                }
                Map<String, List<String>> filesWrittenFragment = new HashMap<String, List<String>>();
                filesWrittenFragment = mapper.readValue(fragment, filesWrittenFragment.getClass());
                for (String partition : filesWrittenFragment.keySet()) {
                    if (!filesWritten.containsKey(partition)) {
                        filesWritten.put(partition, new ArrayList<String>());
                    }

                    filesWritten.get(partition).addAll(filesWrittenFragment.get(partition));
                }
            }

            commitInsertWork(handle, filesWritten);
        }
        catch (Exception e) {
            rollbackInsertChanges(handle, filesWritten);
            throw Throwables.propagate(e);
        }
    }

    private void rollbackInsertChanges(HiveInsertTableHandle handle, Map<String, List<String>> filesWritten)
    {
        Path tableLocation = new Path(handle.getTargetPath());
        try {
            for (String partition : filesWritten.keySet()) {
                Path partitionPath;
                if (handle.isOutputTablePartitioned()) {
                    partitionPath = new Path(tableLocation, partition);
                }
                else {
                    partitionPath = tableLocation;
                }
                for (String file : filesWritten.get(partition)) {
                    delete(new Path(partitionPath, file), false);
                }
            }
        }
        catch (IOException e) {
            log.error(String.format("Manually delete files prefixed with '%s' at '%s'. Rollback of changes made during insert failed with %s.",
                                    handle.getFilePrefix(),
                                    handle.getTargetPath(),
                                    e.getStackTrace()));
        }
    }

    private void commitInsertWork(HiveInsertTableHandle handle, Map<String, List<String>> filesWritten) throws PrestoException, IOException, TException
    {
        Path targetLocation = new Path(handle.getTargetPath());

        //Move data from temp locations
        if (handle.hasTemporaryPath()) {
            moveInsertIntoData(handle, filesWritten);
        }

        if (handle.isOutputTablePartitioned()) {
            // Get info about all the existing partitions
            Set<String> partitionsKnown = new HashSet<String>();
            ConnectorPartitionResult result = getPartitions(new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                                                TupleDomain.<ConnectorColumnHandle>all());
            List<ConnectorPartition> partitions = result.getPartitions();
            Iterator<ConnectorPartition> pIter = partitions.iterator();

            while (pIter.hasNext()) {
                HivePartition p = (HivePartition) pIter.next();
                // partitionId will be of form pKey1=value1/pKey2=value2
                String partitionId = p.getPartitionId();

                if (!partitionsKnown.add(partitionId.toString())) {
                    throw new RuntimeException(String.format("Table '%s' has duplicate partitions for '%s'", handle.getTableName(), partitionId));
                }
            }

            Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName());

            findAndRecoverPartitions(targetLocation,
                                     filesWritten.keySet(),
                                     partitionsKnown,
                                     table);
        }
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(tupleDomain, "tupleDomain is null");
        SchemaTableName tableName = getTableName(tableHandle);

        return getPartitions(tableName, tupleDomain);
    }

    public ConnectorPartitionResult getPartitions(SchemaTableName tableName, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        List<FieldSchema> partitionKeys;
        Optional<HiveBucket> bucket;

        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

            String protectMode = table.getParameters().get(ProtectMode.PARAMETER_NAME);
            if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                throw new TableOfflineException(tableName);
            }

            String prestoOffline = table.getParameters().get(PRESTO_OFFLINE);
            if (!isNullOrEmpty(prestoOffline)) {
                throw new TableOfflineException(tableName, format("Table '%s' is offline for Presto: %s", tableName, prestoOffline));
            }

            partitionKeys = table.getPartitionKeys();
            bucket = getHiveBucket(table, tupleDomain.extractFixedValues());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        ImmutableMap.Builder<String, ConnectorColumnHandle> partitionKeysByNameBuilder = ImmutableMap.builder();
        List<String> filterPrefix = new ArrayList<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);

            HiveType hiveType = getSupportedHiveType(field.getType());
            HiveColumnHandle columnHandle = new HiveColumnHandle(connectorId, field.getName(), i, hiveType, getType(field.getType()).getName(), -1, true);
            partitionKeysByNameBuilder.put(field.getName(), columnHandle);

            // only add to prefix if all previous keys have a value
            if (filterPrefix.size() == i && !tupleDomain.isNone()) {
                Domain domain = tupleDomain.getDomains().get(columnHandle);
                if (domain != null && domain.getRanges().getRangeCount() == 1) {
                    // We intentionally ignore whether NULL is in the domain since partition keys can never be NULL
                    Range range = getOnlyElement(domain.getRanges());
                    if (range.isSingleValue()) {
                        Comparable<?> value = range.getLow().getValue();
                        checkArgument(value instanceof Boolean || value instanceof Slice || value instanceof Double || value instanceof Long,
                                "Only Boolean, Slice (UTF8 String), Double and Long partition keys are supported");
                        if (value instanceof Slice) {
                            filterPrefix.add(((Slice) value).toStringUtf8());
                        }
                        else {
                            filterPrefix.add(value.toString());
                        }
                    }
                }
            }
        }

        // fetch the partition names
        List<String> partitionNames;
        try {
            if (partitionKeys.isEmpty()) {
                partitionNames = ImmutableList.of(UNPARTITIONED_ID);
            }
            else if (filterPrefix.isEmpty()) {
                partitionNames = metastore.getPartitionNames(tableName.getSchemaName(), tableName.getTableName());
            }
            else {
                partitionNames = metastore.getPartitionNamesByParts(tableName.getSchemaName(), tableName.getTableName(), filterPrefix);
            }
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        // do a final pass to filter based on fields that could not be used to build the prefix
        Map<String, ConnectorColumnHandle> partitionKeysByName = partitionKeysByNameBuilder.build();
        List<ConnectorPartition> partitions = FluentIterable.from(partitionNames)
                .transform(toPartition(tableName, partitionKeysByName, bucket, timeZone, typeManager))
                .filter(partitionMatches(tupleDomain))
                .filter(ConnectorPartition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ConnectorColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionKeysByName.values()))));
        }

        return new ConnectorPartitionResult(partitions, remainingTupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> connectorPartitions)
    {
        HiveTableHandle hiveTableHandle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");

        checkNotNull(connectorPartitions, "connectorPartitions is null");
        List<HivePartition> partitions = Lists.transform(connectorPartitions, new Function<ConnectorPartition, HivePartition>()
        {
            @Override
            public HivePartition apply(ConnectorPartition partition)
            {
                return checkType(partition, HivePartition.class, "partition");
            }
        });

        HivePartition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }
        SchemaTableName tableName = partition.getTableName();
        Optional<HiveBucket> bucket = partition.getBucket();

        // sort partitions
        partitions = Ordering.natural().onResultOf(partitionIdGetter()).reverse().sortedCopy(partitions);

        Table table;
        Iterable<HivePartitionMetadata> hivePartitions;
        try {
            table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            hivePartitions = getPartitionMetadata(table, tableName, partitions);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        return new HiveSplitSourceProvider(connectorId,
                table,
                hivePartitions,
                bucket,
                maxSplitSize,
                maxOutstandingSplits,
                maxSplitIteratorThreads,
                hdfsEnvironment,
                namenodeStats,
                directoryLister,
                executor,
                maxPartitionBatchSize,
                hiveTableHandle.getSession(),
                maxInitialSplitSize,
                maxInitialSplits,
                recursiveDfsWalkerEnabled).get();
    }

    private Iterable<HivePartitionMetadata> getPartitionMetadata(final Table table, final SchemaTableName tableName, List<HivePartition> partitions)
            throws NoSuchObjectException
    {
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        if (partitions.size() == 1) {
            HivePartition firstPartition = getOnlyElement(partitions);
            if (firstPartition.getPartitionId().equals(UNPARTITIONED_ID)) {
                return ImmutableList.of(new HivePartitionMetadata(firstPartition, UNPARTITIONED_PARTITION));
            }
        }

        Iterable<List<HivePartition>> partitionNameBatches = partitionExponentially(partitions, minPartitionBatchSize, maxPartitionBatchSize);
        Iterable<List<HivePartitionMetadata>> partitionBatches = transform(partitionNameBatches, new Function<List<HivePartition>, List<HivePartitionMetadata>>()
        {
            @Override
            public List<HivePartitionMetadata> apply(List<HivePartition> partitionBatch)
            {
                Exception exception = null;
                for (int attempt = 0; attempt < 10; attempt++) {
                    try {
                        Map<String, Partition> partitions = metastore.getPartitionsByNames(
                                tableName.getSchemaName(),
                                tableName.getTableName(),
                                Lists.transform(partitionBatch, partitionIdGetter()));
                        checkState(partitionBatch.size() == partitions.size(), "expected %s partitions but found %s", partitionBatch.size(), partitions.size());

                        ImmutableList.Builder<HivePartitionMetadata> results = ImmutableList.builder();
                        for (HivePartition hivePartition : partitionBatch) {
                            Partition partition = partitions.get(hivePartition.getPartitionId());
                            checkState(partition != null, "Partition %s was not loaded", hivePartition.getPartitionId());

                            // verify all partition is online
                            String protectMode = partition.getParameters().get(ProtectMode.PARAMETER_NAME);
                            String partName = makePartName(table.getPartitionKeys(), partition.getValues());
                            if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                                throw new PartitionOfflineException(tableName, partName);
                            }
                            String prestoOffline = partition.getParameters().get(PRESTO_OFFLINE);
                            if (!isNullOrEmpty(prestoOffline)) {
                                throw new PartitionOfflineException(tableName, partName, format("Partition '%s' is offline for Presto: %s", partName, prestoOffline));
                            }

                            // Verify that the partition schema matches the table schema.
                            // Either adding or dropping columns from the end of the table
                            // without modifying existing partitions is allowed, but every
                            // but every column that exists in both the table and partition
                            // must have the same type.
                            List<FieldSchema> tableColumns = table.getSd().getCols();
                            List<FieldSchema> partitionColumns = partition.getSd().getCols();
                            for (int i = 0; i < min(partitionColumns.size(), tableColumns.size()); i++) {
                                String tableType = tableColumns.get(i).getType();
                                String partitionType = partitionColumns.get(i).getType();
                                if (!tableType.equals(partitionType)) {
                                    throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH.toErrorCode(), format(
                                            "Table '%s' partition '%s' column '%s' type '%s' does not match table column type '%s'",
                                            tableName,
                                            partName,
                                            partitionColumns.get(i).getName(),
                                            partitionType,
                                            tableType));
                                }
                            }

                            results.add(new HivePartitionMetadata(hivePartition, partition));
                        }

                        return results.build();
                    }
                    catch (PrestoException | NoSuchObjectException | NullPointerException | IllegalStateException | IllegalArgumentException e) {
                        throw Throwables.propagate(e);
                    }
                    catch (MetaException | RuntimeException e) {
                        exception = e;
                        log.debug("getPartitions attempt %s failed, will retry. Exception: %s", attempt, e.getMessage());
                    }

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
                assert exception != null; // impossible
                throw Throwables.propagate(exception);
            }
        });
        return concat(partitionBatches);
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle instanceof HiveTableHandle && ((HiveTableHandle) tableHandle).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorColumnHandle columnHandle)
    {
        return columnHandle instanceof HiveColumnHandle && ((HiveColumnHandle) columnHandle).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split instanceof HiveSplit && ((HiveSplit) split).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle handle)
    {
        return (handle instanceof HiveOutputTableHandle) && ((HiveOutputTableHandle) handle).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle tableHandle)
    {
        return (tableHandle instanceof HiveInsertTableHandle) && ((HiveInsertTableHandle) tableHandle).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return false;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return HiveTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorColumnHandle> getColumnHandleClass()
    {
        return HiveColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return HiveSplit.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return HiveOutputTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return HiveInsertTableHandle.class;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, final TypeManager typeManager)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (FieldSchema field : concat(table.getSd().getCols(), table.getPartitionKeys())) {
            if (field.getComment() != null) {
                builder.put(field.getName(), field.getComment());
            }
        }
        final Map<String, String> columnComment = builder.build();

        return new Function<HiveColumnHandle, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(HiveColumnHandle input)
            {
                return new ColumnMetadata(
                        input.getName(),
                        typeManager.getType(input.getTypeName()),
                        input.getOrdinalPosition(),
                        input.isPartitionKey(),
                        columnComment.get(input.getName()),
                        false);
            }
        };
    }

    private static Function<String, HivePartition> toPartition(
            final SchemaTableName tableName,
            final Map<String, ConnectorColumnHandle> columnsByName,
            final Optional<HiveBucket> bucket,
            final DateTimeZone timeZone,
            final TypeManager typeManager)
    {
        return new Function<String, HivePartition>()
        {
            @Override
            public HivePartition apply(String partitionId)
            {
                try {
                    if (partitionId.equals(UNPARTITIONED_ID)) {
                        return new HivePartition(tableName);
                    }

                    ImmutableMap.Builder<ConnectorColumnHandle, Comparable<?>> builder = ImmutableMap.builder();
                    for (Entry<String, String> entry : makeSpecFromName(partitionId).entrySet()) {
                        ConnectorColumnHandle handle = columnsByName.get(entry.getKey());
                        checkArgument(handle != null, "Invalid partition key %s in partition %s", entry.getKey(), partitionId);
                        HiveColumnHandle columnHandle = checkType(handle, HiveColumnHandle.class, "handle");

                        String value = entry.getValue();
                        Type type = typeManager.getType(columnHandle.getTypeName());
                        if (BOOLEAN.equals(type)) {
                            if (value.isEmpty()) {
                                builder.put(columnHandle, false);
                            }
                            else {
                                builder.put(columnHandle, parseBoolean(value));
                            }
                        }
                        else if (BIGINT.equals(type)) {
                            if (value.isEmpty()) {
                                builder.put(columnHandle, 0L);
                            }
                            else if (columnHandle.getHiveType().equals(HiveType.HIVE_TIMESTAMP)) {
                                builder.put(columnHandle, parseHiveTimestamp(value, timeZone));
                            }
                            else {
                                builder.put(columnHandle, parseLong(value));
                            }
                        }
                        else if (DOUBLE.equals(type)) {
                            if (value.isEmpty()) {
                                builder.put(columnHandle, 0.0);
                            }
                            else {
                                builder.put(columnHandle, parseDouble(value));
                            }
                        }
                        else if (VARCHAR.equals(type)) {
                            builder.put(columnHandle, utf8Slice(value));
                        }
                        else {
                            throw new IllegalArgumentException(format("Unsupported partition type [%s] for partition: %s", type, partitionId));
                        }
                    }

                    return new HivePartition(tableName, partitionId, builder.build(), bucket);
                }
                catch (MetaException e) {
                    // invalid partition id
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static Predicate<HivePartition> partitionMatches(final TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        return new Predicate<HivePartition>()
        {
            @Override
            public boolean apply(HivePartition partition)
            {
                if (tupleDomain.isNone()) {
                    return false;
                }
                for (Entry<ConnectorColumnHandle, Comparable<?>> entry : partition.getKeys().entrySet()) {
                    Domain allowedDomain = tupleDomain.getDomains().get(entry.getKey());
                    if (allowedDomain != null && !allowedDomain.includesValue(entry.getValue())) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Partition the given list in exponentially (power of 2) increasing batch sizes starting at 1 up to maxBatchSize
     */
    private static <T> Iterable<List<T>> partitionExponentially(final List<T> values, final int minBatchSize, final int maxBatchSize)
    {
        return new Iterable<List<T>>()
        {
            @Override
            public Iterator<List<T>> iterator()
            {
                return new AbstractIterator<List<T>>()
                {
                    private int currentSize = minBatchSize;
                    private final Iterator<T> iterator = values.iterator();

                    @Override
                    protected List<T> computeNext()
                    {
                        if (!iterator.hasNext()) {
                            return endOfData();
                        }

                        int count = 0;
                        ImmutableList.Builder<T> builder = ImmutableList.builder();
                        while (iterator.hasNext() && count < currentSize) {
                            builder.add(iterator.next());
                            ++count;
                        }

                        currentSize = min(maxBatchSize, currentSize * 2);
                        return builder.build();
                    }
                };
            }
        };
    }

    private String createTemporaryPath(Path targetPath)
    {
        // use a per-user temporary directory to avoid permission problems
        // TODO: this should use Hadoop UserGroupInformation
        String temporaryPrefix = "/tmp/presto-" + StandardSystemProperty.USER_NAME.value();

        // create a temporary directory on the same filesystem
        Path temporaryRoot = new Path(targetPath, temporaryPrefix);
        Path temporaryPath = new Path(temporaryRoot, randomUUID().toString());
        createDirectories(temporaryPath);

        return temporaryPath.toString();
    }

    private void moveInsertIntoData(HiveInsertTableHandle handle, Map<String, List<String>> filesWritten) throws IOException
    {
        ExecutorService executor = Executors.newFixedThreadPool(
                renameThreadPoolSize,
                new ThreadFactoryBuilder().setNameFormat("hive-client-rename-" + "-%d").build());

        for (String partition : filesWritten.keySet()) {
            Path srcPartition;
            Path destPartition;
            if (handle.isOutputTablePartitioned()) {
                srcPartition = new Path(handle.getTemporaryPath(), partition);
                destPartition = new Path(handle.getTargetPath(), partition);
            }
            else {
                srcPartition = new Path(handle.getTemporaryPath());
                destPartition = new Path(handle.getTargetPath());
            }
            if (!pathExists(destPartition)) {
                createDirectories(destPartition);
            }

            for (String file : filesWritten.get(partition)) {
                final Path srcPath = new Path(srcPartition, file);
                final Path destPath = destPartition;
                executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        rename(srcPath, destPath);
                    }
                });

            }
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                // ignore
            }
          }
    }

    private void findAndRecoverPartitions(Path tableLocation,
            Set<String> partitionsWritten,
            Set<String> partitionsKnown,
            final Table table) throws TException
    {
        List<Partition> commitBatch = new ArrayList<Partition>();
        int recovered = 0;

        for (String partition : partitionsWritten) {
            if (!partitionsKnown.contains(partition)) {
                // create the partition location using tableLocation and the values
                Path partLocation = new Path(tableLocation, partition);
                Partition tpart = metastore.createPartition(table.getDbName(), table.getTableName(), getPartitionValues(partition), null, table, partLocation.toString());
                commitBatch.add(tpart);
                recovered++;

                if (commitBatch.size() >= partionCommitBatchSize) {
                    metastore.addPartitions(commitBatch, table.getDbName(), table.getTableName());
                    commitBatch.clear();
                }
            }
        }

        if (commitBatch.size() > 0) {
            metastore.addPartitions(commitBatch, table.getDbName(), table.getTableName());
            commitBatch.clear();
        }

        log.info("Recovered " + recovered + " partitions");
    }
}
