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

import com.facebook.presto.hive.orc.StreamDescriptor;
import org.joda.time.DateTimeZone;

public final class StreamReaders
{
    private StreamReaders()
    {
    }

    public static StreamReader createStreamReader(StreamDescriptor streamDescriptor, DateTimeZone sessionTimeZone)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanStreamReader(streamDescriptor);
            case BYTE:
                return new ByteStreamReader(streamDescriptor);
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongStreamReader(streamDescriptor);
            case FLOAT:
                return new FloatStreamReader(streamDescriptor);
            case DOUBLE:
                return new DoubleStreamReader(streamDescriptor);
            case BINARY:
            case STRING:
                return new SliceStreamReader(streamDescriptor);
            case TIMESTAMP:
                return new TimestampStreamReader(streamDescriptor);
            case STRUCT:
            case LIST:
            case MAP:
                return new JsonStreamReader(streamDescriptor, sessionTimeZone);
            case UNION:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
