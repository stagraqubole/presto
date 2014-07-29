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
package com.facebook.presto.metadata;

import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.WindowFunctionDefinition;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class FunctionInfo
{
    private final Signature signature;
    private final String description;
    private final boolean hidden;

    private final boolean isAggregate;
    private final Type intermediateType;
    private final AggregationFunction aggregationFunction;

    private final MethodHandle methodHandle;
    private final boolean deterministic;
    private final FunctionBinder functionBinder;

    private final boolean isWindow;
    private final WindowFunctionSupplier windowFunctionSupplier;

    public FunctionInfo(Signature signature, String description, WindowFunctionSupplier windowFunctionSupplier)
    {
        this.signature = signature;
        this.description = description;
        this.hidden = false;
        this.deterministic = true;

        this.isAggregate = false;
        this.intermediateType = null;
        this.aggregationFunction = null;
        this.methodHandle = null;
        this.functionBinder = null;

        this.isWindow = true;
        this.windowFunctionSupplier = checkNotNull(windowFunctionSupplier, "windowFunction is null");
    }

    public FunctionInfo(Signature signature, String description, Type intermediateType, AggregationFunction function)
    {
        this.signature = signature;
        this.description = description;
        this.hidden = false;
        this.intermediateType = intermediateType;
        this.aggregationFunction = function;
        this.isAggregate = true;
        this.methodHandle = null;
        this.deterministic = true;
        this.functionBinder = null;
        this.isWindow = false;
        this.windowFunctionSupplier = null;
    }

    public FunctionInfo(Signature signature, String description, boolean hidden, MethodHandle function, boolean deterministic, FunctionBinder functionBinder)
    {
        this.signature = signature;
        this.description = description;
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.functionBinder = functionBinder;

        this.isAggregate = false;
        this.intermediateType = null;
        this.aggregationFunction = null;

        this.isWindow = false;
        this.windowFunctionSupplier = null;
        this.methodHandle = checkNotNull(function, "function is null");
    }

    public Signature getSignature()
    {
        return signature;
    }

    public QualifiedName getName()
    {
        return QualifiedName.of(signature.getName());
    }

    public String getDescription()
    {
        return description;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isAggregate()
    {
        return isAggregate;
    }

    public boolean isWindow()
    {
        return isWindow;
    }

    public boolean isScalar()
    {
        return !isWindow && !isAggregate;
    }

    public boolean isApproximate()
    {
        return signature.isApproximate();
    }

    public Type getReturnType()
    {
        return signature.getReturnType();
    }

    public List<Type> getArgumentTypes()
    {
        return signature.getArgumentTypes();
    }

    public Type getIntermediateType()
    {
        return intermediateType;
    }

    public WindowFunctionDefinition bindWindowFunction(List<Integer> inputs)
    {
        checkState(isWindow, "not a window function");
        return window(windowFunctionSupplier, inputs);
    }

    public AggregationFunctionDefinition bind(List<Integer> inputs, Optional<Integer> mask, Optional<Integer> sampleWeight, double confidence)
    {
        checkState(isAggregate, "function is not an aggregate");
        return aggregation(aggregationFunction, inputs, mask, sampleWeight, confidence);
    }

    public AggregationFunction getAggregationFunction()
    {
        checkState(aggregationFunction != null, "not an aggregation function");
        return aggregationFunction;
    }

    public MethodHandle getMethodHandle()
    {
        checkState(methodHandle != null, "not a scalar function or operator");
        return methodHandle;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public FunctionBinder getFunctionBinder()
    {
        return functionBinder;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final FunctionInfo other = (FunctionInfo) obj;
        return Objects.equal(this.signature, other.signature) &&
                Objects.equal(this.isAggregate, other.isAggregate) &&
                Objects.equal(this.isWindow, other.isWindow);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(signature, isAggregate, isWindow);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("signature", signature)
                .add("isAggregate", isAggregate)
                .add("isWindow", isWindow)
                .toString();
    }

    public static Function<FunctionInfo, QualifiedName> nameGetter()
    {
        return new Function<FunctionInfo, QualifiedName>()
        {
            @Override
            public QualifiedName apply(FunctionInfo input)
            {
                return input.getName();
            }
        };
    }

    public static Function<FunctionInfo, Signature> handleGetter()
    {
        return new Function<FunctionInfo, Signature>()
        {
            @Override
            public Signature apply(FunctionInfo input)
            {
                return input.getSignature();
            }
        };
    }

    public static Predicate<FunctionInfo> isAggregationPredicate()
    {
        return new Predicate<FunctionInfo>()
        {
            @Override
            public boolean apply(FunctionInfo functionInfo)
            {
                return functionInfo.isAggregate();
            }
        };
    }

    public static Predicate<FunctionInfo> isHiddenPredicate()
    {
        return new Predicate<FunctionInfo>()
        {
            @Override
            public boolean apply(FunctionInfo functionInfo)
            {
                return functionInfo.isHidden();
            }
        };
    }
}
