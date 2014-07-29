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
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.RowPageBuilder.rowPageBuilder;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestAlignmentOperator
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
    public void testAlignment()
            throws Exception
    {
        Operator operator = createAlignmentOperator();

        List<Page> expected = rowPagesBuilder(VARCHAR, BIGINT)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .pageBreak()
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .pageBreak()
                .row("alice", 8)
                .row("bob", 9)
                .row("charlie", 10)
                .row("dave", 11)
                .build();

        assertOperatorEquals(operator, expected);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        Operator operator = createAlignmentOperator();

        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // read first page
        assertPageEquals(operator.getTypes(), operator.getOutput(), rowPageBuilder(VARCHAR, BIGINT)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .build());

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // read second page
        assertPageEquals(operator.getTypes(), operator.getOutput(), rowPageBuilder(VARCHAR, BIGINT)
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .build());

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // finish
        operator.finish();

        // verify state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }

    private Operator createAlignmentOperator()
    {
        Iterable<Block> channel0 = ImmutableList.of(
                createStringsBlock("alice", "bob", "charlie", "dave"),
                createStringsBlock("alice", "bob", "charlie", "dave"),
                createStringsBlock("alice", "bob", "charlie", "dave"));

        Iterable<Block> channel1 = ImmutableList.of(createLongSequenceBlock(0, 12));

        OperatorContext operatorContext = driverContext.addOperatorContext(0, AlignmentOperator.class.getSimpleName());
        return new AlignmentOperator(operatorContext, ImmutableList.of(VARCHAR, BIGINT), ImmutableList.of(channel0, channel1));
    }
}
