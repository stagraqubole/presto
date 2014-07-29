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

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class ApproximatePercentileAggregation
        implements AggregationFunction
{
    private final Type parameterType;

    public ApproximatePercentileAggregation(Type parameterType)
    {
        this.parameterType = parameterType;
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.of(parameterType, DOUBLE);
    }

    @Override
    public Type getFinalType()
    {
        return getOutputType(parameterType);
    }

    @Override
    public Type getIntermediateType()
    {
        return VARCHAR;
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public ApproximatePercentileGroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        checkArgument(confidence == 1.0, "approximate percentile does not support approximate queries");
        checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        return new ApproximatePercentileGroupedAccumulator(argumentChannels[0], argumentChannels[1], parameterType, maskChannel);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        checkArgument(confidence == 1.0, "approximate percentile does not support approximate queries");
        return new ApproximatePercentileGroupedAccumulator(-1, -1, parameterType, Optional.<Integer>absent());
    }

    public static class ApproximatePercentileGroupedAccumulator
            implements GroupedAccumulator
    {
        private final int valueChannel;
        private final int percentileChannel;
        private final Type parameterType;
        private final ObjectBigArray<DigestAndPercentile> digests;
        private final Optional<Integer> maskChannel;
        private long sizeOfValues;

        public ApproximatePercentileGroupedAccumulator(int valueChannel, int percentileChannel, Type parameterType, Optional<Integer> maskChannel)
        {
            this.digests = new ObjectBigArray<>();
            this.valueChannel = valueChannel;
            this.percentileChannel = percentileChannel;
            this.parameterType = parameterType;
            this.maskChannel = maskChannel;
        }

        @Override
        public long getEstimatedSize()
        {
            return digests.sizeOf() + sizeOfValues;
        }

        @Override
        public Type getFinalType()
        {
            return getOutputType(parameterType);
        }

        @Override
        public Type getIntermediateType()
        {
            return VARCHAR;
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            checkArgument(percentileChannel != -1, "Raw input is not allowed for a final aggregation");

            digests.ensureCapacity(groupIdsBlock.getGroupCount());

            Block values = page.getBlock(valueChannel);
            Block percentiles = page.getBlock(percentileChannel);
            Block masks = null;
            if (maskChannel.isPresent()) {
                masks = page.getBlock(maskChannel.get());
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long groupId = groupIdsBlock.getGroupId(position);

                // skip null values
                if (!values.isNull(position) && (masks == null || BOOLEAN.getBoolean(masks, position))) {
                    DigestAndPercentile currentValue = digests.get(groupId);
                    if (currentValue == null) {
                        currentValue = new DigestAndPercentile(new QuantileDigest(0.01));
                        digests.set(groupId, currentValue);
                        sizeOfValues += currentValue.getDigest().estimatedInMemorySizeInBytes();
                    }

                    sizeOfValues -= currentValue.getDigest().estimatedInMemorySizeInBytes();
                    addValue(currentValue.getDigest(), position, values, parameterType);
                    sizeOfValues += currentValue.getDigest().estimatedInMemorySizeInBytes();

                    // use last non-null percentile
                    if (!percentiles.isNull(position)) {
                        currentValue.setPercentile(DOUBLE.getDouble(percentiles, position));
                    }
                }
            }
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block intermediates)
        {
            checkArgument(percentileChannel == -1, "Intermediate input is only allowed for a final aggregation");

            digests.ensureCapacity(groupIdsBlock.getGroupCount());

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                if (!intermediates.isNull(position)) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    DigestAndPercentile currentValue = digests.get(groupId);
                    if (currentValue == null) {
                        currentValue = new DigestAndPercentile(new QuantileDigest(0.01));
                        digests.set(groupId, currentValue);
                        sizeOfValues += currentValue.getDigest().estimatedInMemorySizeInBytes();
                    }

                    SliceInput input = VARCHAR.getSlice(intermediates, position).getInput();

                    // read digest
                    sizeOfValues -= currentValue.getDigest().estimatedInMemorySizeInBytes();
                    currentValue.getDigest().merge(QuantileDigest.deserialize(input));
                    sizeOfValues += currentValue.getDigest().estimatedInMemorySizeInBytes();

                    // read percentile
                    currentValue.setPercentile(input.readDouble());
                }
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            DigestAndPercentile currentValue = digests.get((long) groupId);
            if (currentValue == null || currentValue.getDigest().getCount() == 0.0) {
                output.appendNull();
            }
            else {
                DynamicSliceOutput sliceOutput = new DynamicSliceOutput(currentValue.getDigest().estimatedSerializedSizeInBytes() + SIZE_OF_DOUBLE);
                // write digest
                currentValue.getDigest().serialize(sliceOutput);
                // write percentile
                sliceOutput.appendDouble(currentValue.getPercentile());

                Slice slice = sliceOutput.slice();
                VARCHAR.writeSlice(output, slice);
            }
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            DigestAndPercentile currentValue = digests.get((long) groupId);
            if (currentValue == null || currentValue.getDigest().getCount() == 0.0) {
                output.appendNull();
            }
            else {
                evaluate(output, parameterType, currentValue.getDigest(), currentValue.getPercentile());
            }
        }
    }

    @Override
    public ApproximatePercentileAccumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        checkArgument(confidence == 1.0, "approximate percentile does not support approximate queries");
        checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        return new ApproximatePercentileAccumulator(argumentChannels[0], argumentChannels[1], parameterType, maskChannel);
    }

    @Override
    public ApproximatePercentileAccumulator createIntermediateAggregation(double confidence)
    {
        checkArgument(confidence == 1.0, "approximate percentile does not support approximate queries");
        return new ApproximatePercentileAccumulator(-1, -1, parameterType, Optional.<Integer>absent());
    }

    public static class ApproximatePercentileAccumulator
            implements Accumulator
    {
        private final int valueChannel;
        private final int percentileChannel;
        private final Type parameterType;
        private final Optional<Integer> maskChannel;

        private final QuantileDigest digest = new QuantileDigest(0.01);
        private double percentile = -1;

        public ApproximatePercentileAccumulator(int valueChannel, int percentileChannel, Type parameterType, Optional<Integer> maskChannel)
        {
            this.valueChannel = valueChannel;
            this.percentileChannel = percentileChannel;
            this.parameterType = parameterType;
            this.maskChannel = maskChannel;
        }

        @Override
        public long getEstimatedSize()
        {
            return digest.estimatedInMemorySizeInBytes();
        }

        @Override
        public Type getFinalType()
        {
            return getOutputType(parameterType);
        }

        @Override
        public Type getIntermediateType()
        {
            return VARCHAR;
        }

        @Override
        public void addInput(Page page)
        {
            checkArgument(valueChannel != -1, "Raw input is not allowed for a final aggregation");

            Block values = page.getBlock(valueChannel);
            Block percentiles = page.getBlock(percentileChannel);
            Block masks = null;
            if (maskChannel.isPresent()) {
                masks = page.getBlock(maskChannel.get());
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                if (!values.isNull(position) && (masks == null || BOOLEAN.getBoolean(masks, position))) {
                    addValue(digest, position, values, parameterType);
                    if (!percentiles.isNull(position)) {
                        percentile = DOUBLE.getDouble(percentiles, position);
                    }
                }
            }
        }

        @Override
        public void addIntermediate(Block intermediates)
        {
            checkArgument(valueChannel == -1, "Intermediate input is only allowed for a final aggregation");

            for (int position = 0; position < intermediates.getPositionCount(); position++) {
                if (!intermediates.isNull(position)) {
                    SliceInput input = VARCHAR.getSlice(intermediates, position).getInput();
                    // read digest
                    digest.merge(QuantileDigest.deserialize(input));
                    // read percentile
                    percentile = input.readDouble();
                }
            }
        }

        @Override
        public final Block evaluateIntermediate()
        {
            BlockBuilder out = getIntermediateType().createBlockBuilder(new BlockBuilderStatus());

            if (digest.getCount() == 0.0) {
                out.appendNull();
            }
            else {
                DynamicSliceOutput sliceOutput = new DynamicSliceOutput(digest.estimatedSerializedSizeInBytes() + SIZE_OF_DOUBLE);
                // write digest
                digest.serialize(sliceOutput);
                // write percentile
                sliceOutput.appendDouble(percentile);

                Slice slice = sliceOutput.slice();
                VARCHAR.writeSlice(out, slice);
            }

            return out.build();
        }

        @Override
        public final Block evaluateFinal()
        {
            BlockBuilder out = getFinalType().createBlockBuilder(new BlockBuilderStatus());
            evaluate(out, parameterType, digest, percentile);
            return out.build();
        }
    }

    private static Type getOutputType(Type parameterType)
    {
        if (parameterType == BIGINT) {
            return BIGINT;
        }
        else if (parameterType == DOUBLE) {
            return DOUBLE;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT or DOUBLE");
        }
    }

    private static void addValue(QuantileDigest digest, int position, Block values, Type parameterType)
    {
        long value;
        if (parameterType == BIGINT) {
            value = BIGINT.getLong(values, position);
        }
        else if (parameterType == DOUBLE) {
            value = doubleToSortableLong(DOUBLE.getDouble(values, position));
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT or DOUBLE");
        }

        digest.add(value);
    }

    public static void evaluate(BlockBuilder out, Type parameterType, QuantileDigest digest, double percentile)
    {
        if (digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");

            long value = digest.getQuantile(percentile);

            if (parameterType == BIGINT) {
                BIGINT.writeLong(out, value);
            }
            else if (parameterType == DOUBLE) {
                DOUBLE.writeDouble(out, longToDouble(value));
            }
            else {
                throw new IllegalArgumentException("Expected parameter type to be BIGINT or DOUBLE");
            }
        }
    }

    private static double longToDouble(long value)
    {
        if (value < 0) {
            value ^= 0x7fffffffffffffffL;
        }

        return Double.longBitsToDouble(value);
    }

    private static long doubleToSortableLong(double value)
    {
        long result = Double.doubleToRawLongBits(value);

        if (result < 0) {
            result ^= 0x7fffffffffffffffL;
        }

        return result;
    }

    public static final class DigestAndPercentile
    {
        private final QuantileDigest digest;
        private double percentile = -1.0;

        public DigestAndPercentile(QuantileDigest digest)
        {
            this.digest = digest;
        }

        public QuantileDigest getDigest()
        {
            return digest;
        }

        public double getPercentile()
        {
            return percentile;
        }

        public void setPercentile(double percentile)
        {
            this.percentile = percentile;
        }
    }
}
