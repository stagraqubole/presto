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
package com.facebook.presto.benchmark;

import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;

public class CountAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public CountAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "count_agg", 10, 100);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, "orders", "orderkey");
        AggregationOperatorFactory aggregationOperator = new AggregationOperatorFactory(1, Step.SINGLE, ImmutableList.of(aggregation(COUNT, ImmutableList.of(0), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0)));
        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
