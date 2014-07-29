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
import com.facebook.presto.operator.LimitOperator.LimitOperatorFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.appendSampleWeight;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestLimitOperator
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
    public void testSampledLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(2, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();
        input = appendSampleWeight(input, 2);

        OperatorFactory operatorFactory = new LimitOperatorFactory(0, ImmutableList.of(BIGINT, BIGINT), 5, Optional.of(input.get(0).getChannelCount() - 1));
        Operator operator = operatorFactory.createOperator(driverContext);

        List<Page> expected = rowPagesBuilder(BIGINT, BIGINT)
                .row(1, 2)
                .row(2, 2)
                .pageBreak()
                .row(4, 1)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLimitWithPageAlignment()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();

        OperatorFactory operatorFactory = new LimitOperatorFactory(0, ImmutableList.of(BIGINT), 5, Optional.<Integer>absent());
        Operator operator = operatorFactory.createOperator(driverContext);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLimitWithBlockView()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();

        OperatorFactory operatorFactory = new LimitOperatorFactory(0, ImmutableList.of(BIGINT), 6, Optional.<Integer>absent());
        Operator operator = operatorFactory.createOperator(driverContext);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(1, 6)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, input, expected);
    }
}
