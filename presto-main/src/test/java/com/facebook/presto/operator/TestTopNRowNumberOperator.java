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
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.TopNRowNumberOperator.TopNRowNumberOperatorFactory;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestTopNRowNumberOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        ConnectorSession session = new ConnectorSession("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");
        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testTopNRowNumber()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.3)
                .row(2, 0.2)
                .row(3, 0.1)
                .row(3, 0.91)
                .pageBreak()
                .row(1, 0.4)
                .pageBreak()
                .row(1, 0.5)
                .row(1, 0.6)
                .row(2, 0.7)
                .row(2, 0.8)
                .pageBreak()
                .row(2, 0.9)
                .build();

        TopNRowNumberOperatorFactory operatorFactory = new TopNRowNumberOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.3, 1, 1)
                .row(0.4, 1, 2)
                .row(0.5, 1, 3)
                .row(0.2, 2, 1)
                .row(0.7, 2, 2)
                .row(0.8, 2, 3)
                .row(0.1, 3, 1)
                .row(0.91, 3, 2)
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
