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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TimestampWithTimeZoneOperators
{
    private TimestampWithTimeZoneOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long min,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long max)
    {
        return unpackMillisUtc(min) <= unpackMillisUtc(value) && unpackMillisUtc(value) <= unpackMillisUtc(max);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static long castToDate(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        // round down the current timestamp to days
        ISOChronology chronology = unpackChronology(value);
        long date = chronology.dayOfYear().roundFloor(unpackMillisUtc(value));
        // date is currently midnight in timezone of the original value
        // convert to UTC
        return date + chronology.getZone().getOffset(date);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        int millis = modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
        return packDateTimeWithZone(millis, unpackZoneKey(value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToSlice(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return Slices.copiedBuffer(printTimestampWithTimeZone(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        long millis = unpackMillisUtc(value);
        return (int) (millis ^ (millis >>> 32));
    }
}
