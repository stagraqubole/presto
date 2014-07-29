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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final Step step;
        private final List<AggregationFunctionDefinition> functionDefinitions;
        private final List<Type> types;
        private boolean closed;

        public AggregationOperatorFactory(int operatorId, Step step, List<AggregationFunctionDefinition> functionDefinitions)
        {
            this.operatorId = operatorId;
            this.step = step;
            this.functionDefinitions = functionDefinitions;
            this.types = toTypes(step, functionDefinitions);
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(operatorContext, step, functionDefinitions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final List<Aggregator> aggregates;

    private State state = State.NEEDS_INPUT;

    public AggregationOperator(OperatorContext operatorContext, Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(step, "step is null");
        checkNotNull(functionDefinitions, "functionDefinitions is null");

        this.types = toTypes(step, functionDefinitions);
        MemoryManager memoryManager = new MemoryManager(operatorContext);

        // wrapper each function with an aggregator
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            builder.add(new Aggregator(functionDefinition, step, memoryManager));
        }
        aggregates = builder.build();
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
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        checkNotNull(page, "page is null");

        for (Aggregator aggregate : aggregates) {
            aggregate.processPage(page);
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // project results into output blocks
        Block[] blocks = new Block[aggregates.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = aggregates.get(i).evaluate();
        }
        state = State.FINISHED;
        return new Page(blocks);
    }

    private static List<Type> toTypes(Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                types.add(functionDefinition.getFunction().getFinalType());
            }
            else {
                types.add(functionDefinition.getFunction().getIntermediateType());
            }
        }
        return types.build();
    }

    private static class Aggregator
    {
        private final Accumulator aggregation;
        private final Step step;
        private final MemoryManager memoryManager;

        private final int intermediateChannel;

        private Aggregator(AggregationFunctionDefinition functionDefinition, Step step, MemoryManager memoryManager)
        {
            AggregationFunction function = functionDefinition.getFunction();

            if (step != Step.FINAL) {
                int[] argumentChannels = new int[functionDefinition.getInputs().size()];
                for (int i = 0; i < argumentChannels.length; i++) {
                    argumentChannels[i] = functionDefinition.getInputs().get(i);
                }
                intermediateChannel = -1;
                aggregation = function.createAggregation(
                        functionDefinition.getMask(),
                        functionDefinition.getSampleWeight(),
                        functionDefinition.getConfidence(),
                        argumentChannels);
            }
            else {
                checkArgument(functionDefinition.getInputs().size() == 1, "Expected a single input for an intermediate aggregation");
                intermediateChannel = functionDefinition.getInputs().get(0);
                aggregation = function.createIntermediateAggregation(functionDefinition.getConfidence());
            }
            this.step = step;
            this.memoryManager = memoryManager;
        }

        public Type getType()
        {
            if (step == Step.PARTIAL) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        public void processPage(Page page)
        {
            if (step == Step.FINAL) {
                aggregation.addIntermediate(page.getBlock(intermediateChannel));
            }
            else {
                aggregation.addInput(page);
            }
            if (!memoryManager.canUse(aggregation.getEstimatedSize())) {
                throw new ExceededMemoryLimitException(memoryManager.getMaxMemorySize());
            }
        }

        public Block evaluate()
        {
            if (step == Step.PARTIAL) {
                return aggregation.evaluateIntermediate();
            }
            else {
                return aggregation.evaluateFinal();
            }
        }
    }
}
