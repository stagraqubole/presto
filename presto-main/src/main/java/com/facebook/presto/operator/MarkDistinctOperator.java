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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MarkDistinctOperator
        implements Operator
{
    public static class MarkDistinctOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int[] markDistinctChannels;
        private final List<Type> types;
        private final Optional<Integer> sampleWeightChannel;
        private boolean closed;

        public MarkDistinctOperatorFactory(int operatorId, List<? extends Type> sourceTypes, Collection<Integer> markDistinctChannels, Optional<Integer> sampleWeightChannel)
        {
            this.operatorId = operatorId;
            checkNotNull(markDistinctChannels, "markDistinctChannels is null");
            checkArgument(!markDistinctChannels.isEmpty(), "markDistinctChannels is empty");
            checkNotNull(sampleWeightChannel, "sampleWeightChannel is null");
            this.markDistinctChannels = Ints.toArray(markDistinctChannels);
            this.sampleWeightChannel = sampleWeightChannel;

            this.types = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BOOLEAN)
                    .build();
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MarkDistinctOperator.class.getSimpleName());
            if (sampleWeightChannel.isPresent()) {
                return new MarkDistinctSampledOperator(operatorContext, types, markDistinctChannels, sampleWeightChannel.get());
            }
            else {
                return new MarkDistinctOperator(operatorContext, types, markDistinctChannels);
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final MarkDistinctHash markDistinctHash;

    private Page outputPage;
    private boolean finishing;

    public MarkDistinctOperator(OperatorContext operatorContext, List<Type> types, int[] markDistinctChannels)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(types, "types is null");
        checkArgument(markDistinctChannels.length >= 0, "markDistinctChannels is empty");

        ImmutableList.Builder<Type> markDistinctTypes = ImmutableList.builder();
        for (int channel : markDistinctChannels) {
            markDistinctTypes.add(types.get(channel));
        }
        this.markDistinctHash = new MarkDistinctHash(markDistinctTypes.build(), markDistinctChannels);

        this.types = ImmutableList.copyOf(types);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && outputPage == null;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());
        if (finishing || outputPage != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(outputPage == null, "Operator still has pending output");
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());

        Block markerBlock = markDistinctHash.markDistinctRows(page);

        // add the new boolean column to the page
        Block[] sourceBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[sourceBlocks.length + 1]; // +1 for the single boolean output channel

        System.arraycopy(sourceBlocks, 0, outputBlocks, 0, sourceBlocks.length);
        outputBlocks[sourceBlocks.length] = markerBlock;

        outputPage = new Page(outputBlocks);
    }

    @Override
    public Page getOutput()
    {
        Page result = outputPage;
        outputPage = null;
        return result;
    }
}

class MarkDistinctSampledOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final MarkDistinctHash markDistinctHash;
    private final int sampleWeightChannel;
    private final int markerChannel;

    private int position = -1;
    private Block[] blocks;
    private Block markerBlock;
    private boolean finishing;
    private PageBuilder pageBuilder;
    private long sampleWeight;
    private boolean distinct;

    public MarkDistinctSampledOperator(OperatorContext operatorContext, List<Type> types, int[] markDistinctChannels, int sampleWeightChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(types, "types is null");
        checkArgument(markDistinctChannels.length >= 0, "markDistinctChannels is empty");
        this.sampleWeightChannel = sampleWeightChannel;
        // Add marker at end of columns
        this.markerChannel = types.size() - 1;

        ImmutableList.Builder<Type> markDistinctTypes = ImmutableList.builder();
        for (int channel : markDistinctChannels) {
            markDistinctTypes.add(types.get(channel));
        }
        this.markDistinctHash = new MarkDistinctHash(markDistinctTypes.build(), markDistinctChannels);

        this.types = ImmutableList.copyOf(types);
        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && markerBlock == null && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());
        if (finishing || markerBlock != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(markerBlock == null, "Current page has not been completely processed yet");
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());

        markerBlock = markDistinctHash.markDistinctRows(page);

        position = -1;
        blocks = page.getBlocks();
    }

    private boolean advance()
    {
        if (markerBlock == null) {
            return false;
        }

        if (distinct && sampleWeight > 1) {
            distinct = false;
            sampleWeight--;
            return true;
        }

        position++;
        if (position >= markerBlock.getPositionCount()) {
            markerBlock = null;
            Arrays.fill(blocks, null);
            return false;
        }
        else {
            sampleWeight = BIGINT.getLong(blocks[sampleWeightChannel], position);
            distinct = BOOLEAN.getBoolean(markerBlock, position);
            return true;
        }
    }

    @Override
    public Page getOutput()
    {
        // Build the weight block, giving all distinct rows a weight of one. advance() handles splitting rows with weight > 1, if they're distinct
        while (!pageBuilder.isFull() && advance()) {
            for (int i = 0; i < blocks.length; i++) {
                BlockBuilder builder = pageBuilder.getBlockBuilder(i);
                if (i == sampleWeightChannel) {
                    if (distinct) {
                        BIGINT.writeLong(builder, 1);
                    }
                    else {
                        BIGINT.writeLong(builder, sampleWeight);
                    }
                }
                else {
                    types.get(i).appendTo(blocks[i], position, builder);
                }
            }
            BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(markerChannel), distinct);
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && markerBlock == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }
}
