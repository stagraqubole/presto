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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;

public class HiveRecordSink
        implements RecordSink
{
    private final int fieldCount;
    @SuppressWarnings("deprecation")
    private final Serializer serializer;
    private RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final List<Type> columnTypes;
    private final List<Boolean> hasDateTimeTypes;
    private final Object row;
    private final int sampleWeightField;
    private final ConnectorSession connectorSession;

    private int field = -1;

    private final boolean isPartitioned;
    private List<String> partitionColNames;
    private List<String> partitionValues;
    private final JobConf conf;
    private final int dataFieldsCount;
    Class<? extends HiveOutputFormat> outputFormatClass = null;

    private final String fileName;
    private final Path basePath;

    private Map<String, List<String>> filesWritten; // filesWritten for each partition
    private LoadingCache<String, RecordWriter> recordWriters;
    private final Properties properties;

    private final ClassLoader classLoader;

    public HiveRecordSink(HiveInsertTableHandle handle, Path target, JobConf conf)
    {
        this(target,
            conf,
            handle.getColumnNames(),
            handle.getDataColumnNames(),
            handle.getColumnTypes(),
            handle.getDataColumnTypes(),
            handle.getOutputFormat(),
            handle.getSerdeLib(),
            handle.getSerdeParameters(),
            handle.getFilePrefix(),
            handle.isOutputTablePartitioned(),
            handle.getConnectorSession());

        if (isPartitioned) {
            partitionColNames = handle.getPartitionColumnNames();
            partitionValues = new ArrayList<String>(partitionColNames.size());
        }
    }

    public HiveRecordSink(HiveOutputTableHandle handle, Path target, JobConf conf)
    {
        this(target,
            conf,
            handle.getColumnNames(),
            handle.getColumnNames(), // CTAS doesnt support partitioning => all cols are data columns
            handle.getColumnTypes(),
            handle.getColumnTypes(),
            handle.getHiveStorageFormat().getOutputFormat(),
            handle.getHiveStorageFormat().getSerDe(),
            null,
            "",
            false,
            handle.getConnectorSession());
    }

    private HiveRecordSink(Path target,
                             JobConf conf,
                             List<String> columnNames,
                             List<String> dataColumnNames,
                             List<Type> columnTypes,
                             List<Type> dataColumnTypes,
                             String outputFormat,
                             String serdeLib,
                             Map<String, String> serdeParameters,
                             String filePrefix,
                             boolean isPartitioned,
                             ConnectorSession connectorSession)
    {
        checkNotNull(target, "Base path for table data not set");

        basePath = target;
        fieldCount = columnNames.size();
        dataFieldsCount = dataColumnNames.size();

        this.conf = conf;
        this.isPartitioned = isPartitioned;

        sampleWeightField = columnNames.indexOf(SAMPLE_WEIGHT_COLUMN_NAME);
        this.columnTypes = columnTypes;
        this.connectorSession = connectorSession;
        hasDateTimeTypes = columnTypes.stream().map(this::containsDateTime).collect(toList());

        Iterable<String> hiveTypeNames = transform(transform(dataColumnTypes, HiveType::toHiveType), HiveType::getHiveTypeName);

        properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(dataColumnNames));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        if (serdeParameters != null) {
            for (String key : serdeParameters.keySet()) {
                properties.setProperty(key, serdeParameters.get(key));
            }
        }

        try {
            serializer = (Serializer) lookupDeserializer(serdeLib);
            Class<?> clazz = Class.forName(outputFormat);
            outputFormatClass = clazz.asSubclass(HiveOutputFormat.class);
            serializer.initialize(conf, properties);
        }
        catch (ClassNotFoundException | ClassCastException | SerDeException e) {
            throw Throwables.propagate(e);
        }

        // many nodes can be writing to same partition so create node specific filename
        filePrefix = (filePrefix.length() > 0) ? filePrefix + "_" : filePrefix;
        fileName = filePrefix + randomUUID().toString();
        filesWritten = new HashMap<String, List<String>>();
        if (isPartitioned) {
            recordWriters = CacheBuilder.newBuilder()
                    .build(
                            new CacheLoader<String, RecordWriter>() {
                                @Override
                                public RecordWriter load(String path)
                                {
                                    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                                        return getRecordWriter(path);
                                    }
                                }
                            });
            recordWriter = null;
        }
        else {
            createNonPartitionedRecordReader();
        }

        this.classLoader = Thread.currentThread().getContextClassLoader();

        tableInspector = getStandardStructObjectInspector(dataColumnNames, getJavaObjectInspectors(dataColumnTypes));
        structFields = ImmutableList.copyOf(tableInspector.getAllStructFieldRefs());
        row = tableInspector.create();
    }

    private RecordWriter createNonPartitionedRecordReader()
    {
        Path filePath = new Path(basePath, fileName);
        if (!filesWritten.containsKey(UNPARTITIONED_ID)) {
            filesWritten.put(UNPARTITIONED_ID, new ArrayList<String>());
        }

        filesWritten.get(UNPARTITIONED_ID).add(fileName);

        recordWriter = createRecordWriter(filePath, conf, properties, outputFormatClass.getName());
        return recordWriter;
    }

    private RecordWriter getRecordWriter(String pathName)
    {
        String partitionId = pathName.startsWith("/") ? pathName.substring(1) : pathName;
        Path partitionPath = new Path(basePath, partitionId);
        Path filePath = new Path(partitionPath, fileName);
        if (!filesWritten.containsKey(partitionId)) {
            filesWritten.put(partitionId, new ArrayList<String>());
        }
        filesWritten.get(partitionId).add(fileName);

        return createRecordWriter(filePath, conf, properties, outputFormatClass.getName());
    }

    private Deserializer lookupDeserializer(String lib) throws ClassNotFoundException
    {
        Deserializer deserializer = ReflectionUtils.newInstance(conf.getClassByName(lib).
                asSubclass(Deserializer.class), conf);
        return deserializer;
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        checkState(field == -1, "already in record");
        if (sampleWeightField >= 0) {
            tableInspector.setStructFieldData(row, structFields.get(sampleWeightField), sampleWeight);
        }
        field = 0;
        if (sampleWeightField == 0) {
            field++;
        }
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fieldCount, "not all fields set");
        field = -1;

        RecordWriter rw = recordWriter;

        if (isPartitioned) {
            String pathName = FileUtils.makePartName(partitionColNames, partitionValues);
            try {
                rw = recordWriters.get(pathName);
            }
            catch (ExecutionException e) {
                throw Throwables.propagate(e);
            }
            partitionValues.clear();
        }

        try {
            rw.write(serializer.serialize(row, tableInspector));
        }
        catch (SerDeException | IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void appendNull()
    {
        append(null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        append(value);
    }

    @Override
    public void appendLong(long value)
    {
        Type type = columnTypes.get(field);
        if (type.equals(DateType.DATE)) {
            // todo should this be adjusted to midnight in JVM timezone?
            append(new Date(TimeUnit.DAYS.toMillis(value)));
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            append(new Timestamp(value));
        }
        else {
            append(value);
        }
    }

    @Override
    public void appendDouble(double value)
    {
        append(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        Type type = columnTypes.get(field);
        if (type.equals(VarbinaryType.VARBINARY)) {
            append(value);
        }
        else if (isMapType(type) || isArrayType(type)) {
            // Hive expects a List<>/Map<> to write, so decode the value
            BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1, value.length);
            type.writeSlice(blockBuilder, Slices.wrappedBuffer(value));
            Object complexValue = type.getObjectValue(connectorSession, blockBuilder.build(), 0);
            if (hasDateTimeTypes.get(field)) {
                complexValue = translateDateTime(type, complexValue);
            }
            append(complexValue);
        }
        else {
            append(new String(value, UTF_8));
        }
    }

    @Override
    public Collection<Slice> commit()
    {
        checkState(field == -1, "record not finished");
        String partitionsJson = "";

        try {
            if (isPartitioned) {
                for (String path : recordWriters.asMap().keySet()) {
                    recordWriters.get(path).close(false);
                }
            }
            else {
                recordWriter.close(false);
            }
            ObjectMapper mapper = new ObjectMapper();
            partitionsJson = mapper.writeValueAsString(filesWritten);
        }
        catch (IOException | ExecutionException e) {
            throw Throwables.propagate(e);
        }

        return ImmutableList.of(Slices.utf8Slice(partitionsJson)); // partition list will be used in commit of insert to add partitions
    }

    @Override
    public void rollback()
    {
        try {
            recordWriter.close(true);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");

        if (field < dataFieldsCount) {
            tableInspector.setStructFieldData(row, structFields.get(field), value);
            field++;
            if (field == sampleWeightField) {
                field++;
            }
        }
        else {
            // into partition columns now
            if (value != null) {
                partitionValues.add(value.toString());
            }
            else {
                throw new RuntimeException(String.format("Null value for partition column '%s'", partitionColNames.get(field - dataFieldsCount)));
            }
            field++;
            checkState(field != sampleWeightField, "Partition columns not at the end");
        }
    }

    private static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties, String outputFormatName)
    {
        try {
            Object writer = Class.forName(outputFormatName).getConstructor().newInstance();
            return ((HiveOutputFormat<?, ?>) writer).getHiveRecordWriter(conf, target, Text.class, false, properties, Reporter.NULL);
        }
        catch (IOException | ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private static List<ObjectInspector> getJavaObjectInspectors(Iterable<Type> types)
    {
        ImmutableList.Builder<ObjectInspector> list = ImmutableList.builder();
        for (Type type : types) {
            list.add(getJavaObjectInspector(type));
        }
        return list.build();
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return javaLongObjectInspector;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return javaStringObjectInspector;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        else if (type.equals(DateType.DATE)) {
            return javaDateObjectInspector;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        else if (isArrayType(type)) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        else if (isMapType(type)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static boolean isTypeSupported(Type type)
    {
        try {
            getJavaObjectInspector(type);
            return true;
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean containsDateTime(Type type)
    {
        if (isArrayType(type)) {
            return containsDateTime(type.getTypeParameters().get(0));
        }
        if (isMapType(type)) {
            return containsDateTime(type.getTypeParameters().get(0)) || containsDateTime(type.getTypeParameters().get(1));
        }
        return type.equals(DateType.DATE) || type.equals(TimestampType.TIMESTAMP);
    }

    private Object translateDateTime(Type type, Object value)
    {
        if (value == null) {
            return null;
        }
        if (isArrayType(type)) {
            List<Object> newValue = new ArrayList<>();
            Type elementType = type.getTypeParameters().get(0);
            for (Object val : (List<?>) value) {
                newValue.add(translateDateTime(elementType, val));
            }
            return newValue;
        }
        if (isMapType(type)) {
            Map<Object, Object> newValue = new HashMap<>();
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newValue.put(
                        translateDateTime(keyType, entry.getKey()),
                        translateDateTime(valueType, entry.getValue()));
            }
            return newValue;
        }
        if (value instanceof SqlDate) {
            return new Date(TimeUnit.DAYS.toMillis(((SqlDate) value).getDays()));
        }
        if (value instanceof SqlTimestamp) {
            return new Timestamp(((SqlTimestamp) value).getMillisUtc());
        }

        return value;
    }
}
