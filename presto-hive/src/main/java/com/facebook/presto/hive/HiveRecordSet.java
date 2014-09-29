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
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.hive.HiveColumnHandle.isPartitionKeyPredicate;
import static com.facebook.presto.hive.HiveColumnHandle.nativeTypeGetter;
import static com.facebook.presto.hive.HiveUtil.getFirstPrimitiveColumn;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;

public class HiveRecordSet
        implements RecordSet
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    private final HiveSplit split;
    private final List<HiveColumnHandle> columns;
    private final List<Type> columnTypes;
    private final List<Integer> readHiveColumnIndexes;
    private final Path path;
    private final Configuration configuration;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;

    public HiveRecordSet(
            HdfsEnvironment hdfsEnvironment,
            HiveSplit split,
            Iterable<HiveColumnHandle> columns,
            Iterable<? extends HiveRecordCursorProvider> cursorProviders,
            DateTimeZone timeZone,
            TypeManager typeManager)
    {
        this.split = checkNotNull(split, "split is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.columnTypes = ImmutableList.copyOf(Iterables.transform(columns, nativeTypeGetter(typeManager)));
        this.cursorProviders = ImmutableSet.copyOf(checkNotNull(cursorProviders, "cursor providers is null"));
        this.timeZone = checkNotNull(timeZone, "timeZone is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");

        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, not(isPartitionKeyPredicate())));
        if (readColumns.isEmpty()) {
            // for count(*) queries we will have "no" columns we want to read, but since hive doesn't
            // support no columns (it will read all columns instead), we must choose a single column
            HiveColumnHandle primitiveColumn = getFirstPrimitiveColumn(split.getClientId(), split.getSchema());
            readColumns = ImmutableList.of(primitiveColumn);
        }
        readHiveColumnIndexes = new ArrayList<>(transform(readColumns, hiveColumnIndexGetter()));

        this.path = new Path(split.getPath());
        this.configuration = hdfsEnvironment.getConfiguration(path);

        String nullSequence = split.getSchema().getProperty(SERIALIZATION_NULL_FORMAT);
        checkState(nullSequence == null || nullSequence.equals("\\N"), "Only '\\N' supported as null specifier, was '%s'", nullSequence);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public HiveRecordCursor cursor()
    {
        for (HiveRecordCursorProvider provider : cursorProviders) {
            Optional<HiveRecordCursor> cursor = provider.createHiveRecordCursor(
                    split.getClientId(),
                    configuration,
                    split.getSession(),
                    path,
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    columns,
                    split.getPartitionKeys(),
                    split.getTupleDomain(),
                    timeZone,
                    typeManager);

            if (cursor.isPresent()) {
                return cursor.get();
            }
        }

        throw new RuntimeException("Configured cursor providers did not provide a cursor");
    }
}
