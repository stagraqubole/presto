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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.ApproximatePercentileAggregation.ApproximatePercentileAccumulator;
import com.facebook.presto.operator.aggregation.ApproximatePercentileAggregation.ApproximatePercentileGroupedAccumulator;
import com.facebook.presto.operator.aggregation.ApproximatePercentileAggregation.DigestAndPercentile;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public final class ApproximatePercentileAggregations
{
    public static final AggregationFunction LONG_APPROXIMATE_PERCENTILE_AGGREGATION = createIsolatedAggregation(BIGINT);
    public static final AggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION = createIsolatedAggregation(DOUBLE);

    private ApproximatePercentileAggregations() {}

    private static AggregationFunction createIsolatedAggregation(Type parameterType)
    {
        Class<? extends AggregationFunction> functionClass = IsolatedClass.isolateClass(
                AggregationFunction.class,

                ApproximatePercentileAggregation.class,

                ApproximatePercentileGroupedAccumulator.class,

                ApproximatePercentileAccumulator.class,

                DigestAndPercentile.class);

        try {
            return functionClass
                    .getConstructor(Type.class)
                    .newInstance(parameterType);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
