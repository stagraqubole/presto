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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import io.airlift.slice.Slice;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public final class DoubleType
        implements FixedWidthType
{
    public static final DoubleType DOUBLE = new DoubleType();

    public static DoubleType getInstance()
    {
        return DOUBLE;
    }

    private DoubleType()
    {
    }

    @Override
    public String getName()
    {
        return "double";
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Class<?> getJavaType()
    {
        return double.class;
    }

    @Override
    public int getFixedSize()
    {
        return (int) SIZE_OF_DOUBLE;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return block.getDouble(position, 0);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new FixedWidthBlockBuilder(getFixedSize(), blockBuilderStatus);
    }

    @Override
    public BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new FixedWidthBlockBuilder(getFixedSize(), positionCount);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position)
    {
        long value = block.getLong(position, 0);
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        double leftValue = leftBlock.getDouble(leftPosition, 0);
        double rightValue = rightBlock.getDouble(rightPosition, 0);
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeDouble(block.getDouble(position, 0)).closeEntry();
        }
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return block.getDouble(position, 0);
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        blockBuilder.writeDouble(value).closeEntry();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
