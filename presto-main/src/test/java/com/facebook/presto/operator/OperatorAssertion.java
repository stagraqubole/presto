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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public final class OperatorAssertion
{
    private OperatorAssertion()
    {
    }

    public static List<Page> appendSampleWeight(List<Page> input, final int sampleWeight)
    {
        return IterableTransformer.on(input).transform(new Function<Page, Page>()
        {
            @Override
            public Page apply(Page page)
            {
                BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
                for (int i = 0; i < page.getPositionCount(); i++) {
                    BIGINT.writeLong(builder, sampleWeight);
                }
                Block[] blocks = new Block[page.getChannelCount() + 1];
                System.arraycopy(page.getBlocks(), 0, blocks, 0, page.getChannelCount());
                blocks[blocks.length - 1] = builder.build();
                return new Page(blocks);
            }
        }).list();
    }

    public static List<Page> toPages(Operator operator, List<Page> input)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);
        assertEquals(operator.getOutput(), null);

        // process input pages
        for (Page inputPage : input) {
            // read output until input is needed or operator is finished
            while (!operator.needsInput() && !operator.isFinished()) {
                Page outputPage = operator.getOutput();
                assertNotNull(outputPage);
                outputPages.add(outputPage);
            }

            if (operator.isFinished()) {
                break;
            }

            assertEquals(operator.needsInput(), true);
            operator.addInput(inputPage);

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        // finish
        operator.finish();
        assertEquals(operator.needsInput(), false);

        // add remaining output pages
        addRemainingOutputPages(operator, outputPages);
        return outputPages.build();
    }

    public static List<Page> toPages(Operator operator)
    {
        // operator does not have input so should never require input
        assertEquals(operator.needsInput(), false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        addRemainingOutputPages(operator, outputPages);
        return outputPages.build();
    }

    private static void addRemainingOutputPages(Operator operator, ImmutableList.Builder<Page> outputPages)
    {
        // pull remaining output pages
        while (true) {
            // at this point the operator should not need more input
            assertEquals(operator.needsInput(), false);

            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                break;
            }
            outputPages.add(outputPage);
        }

        // verify final state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }

    public static MaterializedResult toMaterializedResult(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    public static void assertOperatorEquals(Operator operator, List<Page> expected)
    {
        List<Page> actual = toPages(operator);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(operator.getTypes(), actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(Operator operator, List<Page> input, List<Page> expected)
    {
        List<Page> actual = toPages(operator, input);
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(operator.getTypes(), actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(Operator operator, MaterializedResult expected)
    {
        List<Page> pages = toPages(operator);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEquals(Operator operator, List<Page> input, MaterializedResult expected)
    {
        List<Page> pages = toPages(operator, input);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);
        assertEquals(actual, expected);
    }

    public static void assertOperatorEqualsIgnoreOrder(Operator operator, List<Page> input, MaterializedResult expected)
    {
        List<Page> pages = toPages(operator, input);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), pages);

        assertEquals(actual.getTypes(), expected.getTypes());
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }
}
