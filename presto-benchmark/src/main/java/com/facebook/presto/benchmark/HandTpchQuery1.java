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

import com.facebook.presto.benchmark.HandTpchQuery1.TpchQuery1Operator.TpchQuery1OperatorFactory;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.AverageAggregations.DOUBLE_AVERAGE;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class HandTpchQuery1
        extends AbstractSimpleOperatorBenchmark
{
    public HandTpchQuery1(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hand_tpch_query_1", 1, 5);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        // select
        //     returnflag,
        //     linestatus,
        //     sum(quantity) as sum_qty,
        //     sum(extendedprice) as sum_base_price,
        //     sum(extendedprice * (1 - discount)) as sum_disc_price,
        //     sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
        //     avg(quantity) as avg_qty,
        //     avg(extendedprice) as avg_price,
        //     avg(discount) as avg_disc,
        //     count(*) as count_order
        // from
        //     lineitem
        // where
        //     shipdate <= '1998-09-02'
        // group by
        //     returnflag,
        //     linestatus
        // order by
        //     returnflag,
        //     linestatus

        OperatorFactory tableScanOperator = createTableScanOperator(
                0,
                "lineitem",
                "returnflag",
                "linestatus",
                "quantity",
                "extendedprice",
                "discount",
                "tax",
                "shipdate");

        TpchQuery1OperatorFactory tpchQuery1Operator = new TpchQuery1OperatorFactory(1);
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                2,
                ImmutableList.of(tpchQuery1Operator.getTypes().get(0), tpchQuery1Operator.getTypes().get(1)),
                Ints.asList(0, 1),
                Step.SINGLE,
                ImmutableList.of(
                        aggregation(LONG_SUM, ImmutableList.of(2), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0),
                        aggregation(DOUBLE_SUM, ImmutableList.of(3), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0),
                        aggregation(DOUBLE_SUM, ImmutableList.of(4), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0),
                        aggregation(LONG_AVERAGE, ImmutableList.of(2), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0),
                        aggregation(DOUBLE_AVERAGE, ImmutableList.of(5), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0),
                        aggregation(DOUBLE_AVERAGE, ImmutableList.of(6), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0),
                        aggregation(COUNT, ImmutableList.of(2), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0)
                        ),
                10_000);

        return ImmutableList.of(tableScanOperator, tpchQuery1Operator, aggregationOperator);
    }

    public static class TpchQuery1Operator
            implements com.facebook.presto.operator.Operator // TODO: use import when Java 7 compiler bug is fixed
    {
        private static final ImmutableList<Type> TYPES = ImmutableList.of(
                VARCHAR,
                VARCHAR,
                BIGINT,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                DOUBLE);

        public static class TpchQuery1OperatorFactory
                implements OperatorFactory
        {
            private final int operatorId;

            public TpchQuery1OperatorFactory(int operatorId)
            {
                this.operatorId = operatorId;
            }

            @Override
            public List<Type> getTypes()
            {
                return TYPES;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TpchQuery1Operator.class.getSimpleName());
                return new TpchQuery1Operator(operatorContext);
            }

            @Override
            public void close()
            {
            }
        }

        private final OperatorContext operatorContext;
        private final PageBuilder pageBuilder;
        private boolean finishing;

        public TpchQuery1Operator(OperatorContext operatorContext)
        {
            this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
            this.pageBuilder = new PageBuilder(TYPES);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public List<Type> getTypes()
        {
            return TYPES;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && pageBuilder.isEmpty();
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean needsInput()
        {
            return !pageBuilder.isFull();
        }

        @Override
        public void addInput(Page page)
        {
            checkNotNull(page, "page is null");
            checkState(!pageBuilder.isFull(), "Output buffer is full");
            checkState(!finishing, "Operator is finished");

            filterAndProjectRowOriented(pageBuilder,
                    page.getBlock(0),
                    page.getBlock(1),
                    page.getBlock(2),
                    page.getBlock(3),
                    page.getBlock(4),
                    page.getBlock(5),
                    page.getBlock(6));
        }

        @Override
        public Page getOutput()
        {
            // only return a page if the page buffer isFull or we are finishing and the page buffer has data
            if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty())) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
            return null;
        }

        private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1998-09-02", UTF_8);

        private static void filterAndProjectRowOriented(PageBuilder pageBuilder,
                Block returnFlagBlock,
                Block lineStatusBlock,
                Block quantityBlock,
                Block extendedPriceBlock,
                Block discountBlock,
                Block taxBlock,
                Block shipDateBlock)
        {
            int rows = returnFlagBlock.getPositionCount();
            for (int position = 0; position < rows; position++) {
                if (shipDateBlock.isNull(position)) {
                    continue;
                }

                Slice shipDate = VARCHAR.getSlice(shipDateBlock, position);

                // where
                //     shipdate <= '1998-09-02'
                if (shipDate.compareTo(MAX_SHIP_DATE) <= 0) {
                    //     returnflag,
                    //     linestatus
                    //     quantity
                    //     extendedprice
                    //     extendedprice * (1 - discount)
                    //     extendedprice * (1 - discount) * (1 + tax)
                    //     discount

                    if (returnFlagBlock.isNull(position)) {
                        pageBuilder.getBlockBuilder(0).appendNull();
                    }
                    else {
                        VARCHAR.appendTo(returnFlagBlock, position, pageBuilder.getBlockBuilder(0));
                    }
                    if (lineStatusBlock.isNull(position)) {
                        pageBuilder.getBlockBuilder(1).appendNull();
                    }
                    else {
                        VARCHAR.appendTo(lineStatusBlock, position, pageBuilder.getBlockBuilder(1));
                    }

                    long quantity = BIGINT.getLong(quantityBlock, position);
                    double extendedPrice = DOUBLE.getDouble(extendedPriceBlock, position);
                    double discount = DOUBLE.getDouble(discountBlock, position);
                    double tax = DOUBLE.getDouble(taxBlock, position);

                    boolean quantityIsNull = quantityBlock.isNull(position);
                    boolean extendedPriceIsNull = extendedPriceBlock.isNull(position);
                    boolean discountIsNull = discountBlock.isNull(position);
                    boolean taxIsNull = taxBlock.isNull(position);

                    if (quantityIsNull) {
                        pageBuilder.getBlockBuilder(2).appendNull();
                    }
                    else {
                        BIGINT.writeLong(pageBuilder.getBlockBuilder(2), quantity);
                    }

                    if (extendedPriceIsNull) {
                        pageBuilder.getBlockBuilder(3).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(3), extendedPrice);
                    }

                    if (extendedPriceIsNull || discountIsNull) {
                        pageBuilder.getBlockBuilder(4).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), extendedPrice * (1 - discount));
                    }

                    if (extendedPriceIsNull || discountIsNull || taxIsNull) {
                        pageBuilder.getBlockBuilder(5).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(5), extendedPrice * (1 - discount) * (1 + tax));
                    }

                    if (discountIsNull) {
                        pageBuilder.getBlockBuilder(6).appendNull();
                    }
                    else {
                        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(6), discount);
                    }
                }
            }
        }
    }

    public static void main(String[] args)
    {
        new HandTpchQuery1(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
