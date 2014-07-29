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
package com.facebook.presto.hive.orc.reader;

import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.DoubleVector;
import com.facebook.presto.hive.orc.stream.FloatStream;
import com.facebook.presto.hive.orc.stream.FloatStreamSource;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class FloatStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int skipSize;
    private int nextBatchSize;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;

    private FloatStreamSource dataStreamSource;
    private FloatStream dataStream;

    public FloatStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void setNextBatchSize(int batchSize)
    {
        skipSize += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (dataStream == null) {
            openStreams();
        }

        if (skipSize != 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                skipSize = presentStream.countBitsSet(skipSize);
            }
            dataStream.skip(skipSize);
        }

        // we could add a float vector but Presto currently doesn't support floats
        DoubleVector floatVector = (DoubleVector) vector;
        if (presentStream == null) {
            Arrays.fill(floatVector.isNull, false);
            dataStream.nextVector(nextBatchSize, floatVector.vector);
        }
        else {
            presentStream.getUnsetBits(nextBatchSize, floatVector.isNull);
            dataStream.nextVector(nextBatchSize, floatVector.vector, floatVector.isNull);
        }

        skipSize = 0;
        nextBatchSize = 0;
    }

    private void openStreams()
            throws IOException
    {
        if (presentStreamSource != null) {
            if (presentStream == null) {
                presentStream = presentStreamSource.openStream();
            }
        }

        dataStream = dataStreamSource.openStream();
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = null;
        dataStreamSource = null;

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, FloatStreamSource.class);

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
