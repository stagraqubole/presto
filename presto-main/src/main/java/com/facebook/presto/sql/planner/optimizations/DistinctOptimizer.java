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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

public class DistinctOptimizer
        implements PlanOptimizer
{
    private static final String HASH_CODE = FunctionRegistry.mangleOperatorName("HASH_CODE");

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (SystemSessionProperties.isOptimizeDistinctAggregationEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Optimizer(idAllocator, symbolAllocator),
                    plan,
                    Optional.empty());
        }

        return plan;
    }

    private static class Optimizer
            extends SimplePlanRewriter<Optional<AggregateInfo>>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Optimizer(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Optional<AggregateInfo>> context)
        {
            // optimize if and only if
            // some aggregation functions have a distinct mask symbol
            // and if not all aggregation functions on same distinct mask symbol (this case handled by SingleDistinctOptimizer)
            Set<Symbol> masks = ImmutableSet.copyOf(node.getMasks().values());
            if (masks.size() != 1 || node.getMasks().size() == node.getAggregations().size()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            AggregateInfo aggregateInfo = new AggregateInfo(node.getGroupBy(), Iterables.getOnlyElement(masks), node.getAggregations(), node.getFunctions());

            PlanNode source = context.rewrite(node.getSource(), Optional.of(aggregateInfo));

            ImmutableMap.Builder<Symbol, FunctionCall> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                FunctionCall functionCall = entry.getValue();
                if (entry.getValue().isDistinct()) {
                    aggregations.put(entry.getKey(), new FunctionCall(functionCall.getName(),
                            functionCall.getWindow(),
                            false,
                            ImmutableList.of(Iterables.getOnlyElement(aggregateInfo.getDistinctAggregateSymbols().values()).toSymbolReference())));
                }
                else {
                    aggregations.put(entry.getKey(), new FunctionCall(functionCall.getName(),
                            functionCall.getWindow(),
                            false,
                            ImmutableList.of(aggregateInfo.getNonDistinctAggregateSymbols().get(entry.getKey()).toSymbolReference())));
                }
            }

            return new AggregationNode(idAllocator.getNextId(),
                    source,
                    node.getGroupBy(),
                    aggregations.build(),
                    node.getFunctions(),
                    Collections.emptyMap(),
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getSampleWeight(),
                    node.getConfidence(),
                    Optional.<Symbol>empty());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Optional<AggregateInfo>> context)
        {
            /*
             * 1. Get outputSymbols of source. Source will be project, we get the outputSymbols
             */
            Optional<AggregateInfo> aggregateInfo = context.get();
            // presence of aggregateInfo => mask also present
            if (aggregateInfo.isPresent() && aggregateInfo.get().getMask().get().equals(node.getMarkerSymbol())) {
                List<Symbol> allSymbols = new ArrayList<>();
                List<Symbol> groupBySymbols = aggregateInfo.get().getGroupBySymbols(); // a
                allSymbols.addAll(groupBySymbols);

                // (allSymbols) - (distinctSymbols of MarkDistinct) = symbols in other aggregations
                List<Symbol> nonDistinctAggregateSymbols = new ArrayList<>(node.getSource().getOutputSymbols());
                if (node.getHashSymbol().isPresent()) {
                    // dont need hashSymbol
                    nonDistinctAggregateSymbols.remove(node.getHashSymbol().get());
                }
                nonDistinctAggregateSymbols.removeAll(node.getDistinctSymbols()); // b
                allSymbols.addAll(nonDistinctAggregateSymbols);

                // Distinct Symbol = DistinctSymbols - GBY symbols
                List<Symbol> leftSymbols = new ArrayList(node.getDistinctSymbols());
                if (!groupBySymbols.isEmpty()) {
                    leftSymbols.removeAll(groupBySymbols);
                }
                Symbol distinctSymbol = Iterables.getOnlyElement(leftSymbols); // c
                allSymbols.add(distinctSymbol);

                Symbol groupSymbol = symbolAllocator.newSymbol("group", BigintType.BIGINT); // g

                List<List<Symbol>> groups = new ArrayList<>();
                // g0 = {allGBY_Symbols + allNonDistinctAggregateSymbols}
                // g1 = {allGBY_Symbols + first Distinct Symbol}
                // symbols present in Group_i will be set, rest will be Null

                //g0
                if (!nonDistinctAggregateSymbols.isEmpty()) {
                    List<Symbol> group0 = new ArrayList<>();
                    group0.addAll(groupBySymbols);
                    group0.addAll(nonDistinctAggregateSymbols);
                    groups.add(group0);
                }

                // g1
                List<Symbol> group1 = new ArrayList<>();
                group1.addAll(groupBySymbols);
                group1.add(distinctSymbol);
                groups.add(group1);

                GroupIdNode groupIdNode = new GroupIdNode(idAllocator.getNextId(),
                        node.getSource(),
                        groups,
                        ImmutableMap.of(),
                        groupSymbol);

                // 2. Add aggregation node
                List<Symbol> groupByKeys = new ArrayList<>();
                groupByKeys.addAll(groupBySymbols);
                groupByKeys.add(distinctSymbol);
                groupByKeys.add(groupSymbol);

                /*
                 * The new AggregateNode now aggregates on the symbols that original AggregationNode did
                 * original one will now aggregate on the output symbols of this new node
                 * This map stores the mapping of such symbols, new maps is as follows
                 * key = output symbol of new aggregation
                 * value = output symbol of corresponding aggregation of original AggregationNode
                 */
                ImmutableMap.Builder<Symbol, Symbol> nonDistinctAggregationSymbolMapBuilder = ImmutableMap.builder();
                ImmutableMap.Builder<Symbol, FunctionCall> aggregations = ImmutableMap.builder();
                ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();
                for (Map.Entry<Symbol, FunctionCall> entry : aggregateInfo.get().getAggregations().entrySet()) {
                    if (!entry.getValue().isDistinct()) {
                        Symbol newSymbol = symbolAllocator.newSymbol(entry.getKey().toSymbolReference(), symbolAllocator.getTypes().get(entry.getKey()));
                        nonDistinctAggregationSymbolMapBuilder.put(newSymbol, entry.getKey());
                        aggregations.put(newSymbol, entry.getValue());
                        functions.put(newSymbol, aggregateInfo.get().getFunctions().get(entry.getKey()));
                    }
                }
                AggregationNode aggregationNode = new AggregationNode(idAllocator.getNextId(),
                        groupIdNode,
                        groupByKeys,
                        aggregations.build(), // aggregations using same args but output symbol different
                        functions.build(),
                        Collections.emptyMap(),
                        ImmutableList.of(groupByKeys),
                        SINGLE,
                        Optional.empty(),
                        1.0,
                        node.getHashSymbol());

                /*
                 * 3. Add new project node that adds if expression
                 *   if aggregate on distinct
                 *      new aggregate = aggregate (if g = 1 then aggregateInputSymbol else null)
                 *   else
                 *      new aggregate = aggregate(if g = 0 then aggregateInputSymbol else null)
                 *
                 * This Project is useful for cases when we aggregate on distinct and non-distinct values of same sybol, eg:
                 *  select a, sum(b), count(c), sum(distinct c) group by a
                 * Without this Project, we would count additional values for count(c)
                 */
                Map<Symbol, Symbol> nonDistinctAggregationsSymboMap = nonDistinctAggregationSymbolMapBuilder.build();
                ImmutableMap.Builder<Symbol, Expression> outputSymbols = ImmutableMap.builder();

                // maps of old to new symbols
                // For each key of outputNonDistinctAggregateSymbols,
                // Higher level aggregation node's aggregaton <key, AggregateExpression> will now have to run AggregateExpression on value of outputNonDistinctAggregateSymbols
                // Same for outputDistinctAggregateSymbols map
                ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbols = ImmutableMap.builder();
                ImmutableMap.Builder<Symbol, Symbol> outputDistinctAggregateSymbols = ImmutableMap.builder();
                for (Symbol symbol : aggregationNode.getOutputSymbols()) {
                    if (distinctSymbol.equals(symbol)) {
                        Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                        outputDistinctAggregateSymbols.put(symbol, newSymbol);

                        Expression expression = createIfExpression(groupSymbol.toSymbolReference(),
                                new Cast(new LongLiteral("1"), "bigint"),
                                ComparisonExpression.Type.EQUAL,
                                symbol.toSymbolReference());
                        outputSymbols.put(newSymbol, expression);
                    }
                    else if (nonDistinctAggregationsSymboMap.containsKey(symbol)) {
                        Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                        outputNonDistinctAggregateSymbols.put(nonDistinctAggregationsSymboMap.get(symbol), newSymbol); // key of this map is key of an aggregatio in AggrNode above, it will now aggregate on this Map's value
                        Expression expression = createIfExpression(groupSymbol.toSymbolReference(),
                                new Cast(new LongLiteral("0"), "bigint"),
                                ComparisonExpression.Type.EQUAL,
                                symbol.toSymbolReference());
                        outputSymbols.put(newSymbol, expression);
                    }
                    else {
                        Expression expression = symbol.toSymbolReference();
                        outputSymbols.put(symbol, expression);
                    }
                }

                // add null assignment for mask
                // unused mask will be removed by PruneUnreferencedOutputs
                outputSymbols.put(aggregateInfo.get().getMask().get(), new NullLiteral());

                aggregateInfo.get().setNonDistinctAggregateSymbols(outputNonDistinctAggregateSymbols.build());
                aggregateInfo.get().setDistinctAggregateSymbols(outputDistinctAggregateSymbols.build());

                // This project node is useful when previous aggregation node calculated a value for injected null values
                // e.g. count function would have resulted in 0 for null value, but another count over this would return 1
                // this Project node sets these values back to null
                return new ProjectNode(idAllocator.getNextId(),
                        aggregationNode,
                        outputSymbols.build());
            }
            return context.defaultRewrite(node, Optional.empty());
        }

        // creates if clause specific to use case here, default value always null
        private SearchedCaseExpression createIfExpression(Expression left, Expression right, ComparisonExpression.Type type, Expression result)
        {
            return new SearchedCaseExpression(ImmutableList.of(createWhenClause(left, right, type, result)),
                    Optional.empty());
        }

        private WhenClause createWhenClause(Expression left, Expression right, com.facebook.presto.sql.tree.ComparisonExpression.Type type, Expression result)
        {
            return new WhenClause(new ComparisonExpression(type, left, right),
                    result);
        }

        // below methods from HashGenerationOptimizer with slight modification
        private static Expression getHashFunctionCall(Expression previousHashValue, Symbol symbol)
        {
            FunctionCall functionCall = new FunctionCall(
                    QualifiedName.of(HASH_CODE),
                    Optional.empty(),
                    false,
                    ImmutableList.of(symbol.toSymbolReference()));
            List<Expression> arguments = ImmutableList.of(previousHashValue, orNullHashCode(functionCall));
            return new FunctionCall(QualifiedName.of("combine_hash"), arguments);
        }

        private static Expression orNullHashCode(Expression expression)
        {
            return new CoalesceExpression(expression, new LongLiteral(String.valueOf(TypeUtils.NULL_HASH_CODE)));
        }
    }

    private static class AggregateInfo
    {
        private List<Symbol> groupBySymbols;
        private Optional<Symbol> mask = Optional.empty();
        private Map<Symbol, FunctionCall> aggregations;
        private Map<Symbol, Signature> functions;

        // Filled on the way back, these are the symbols corresponding to their distinct or non-distinct original symbols
        private Map<Symbol, Symbol> nonDistinctAggregateSymbols;
        private Map<Symbol, Symbol> distinctAggregateSymbols;

        public AggregateInfo(List<Symbol> groupBySymbols, Symbol mask, Map<Symbol, FunctionCall> aggregations, Map<Symbol, Signature> functions)
        {
            this.groupBySymbols = ImmutableList.copyOf(groupBySymbols);

            if (mask != null) {
                this.mask = Optional.of(mask);
            }

            this.aggregations = ImmutableMap.copyOf(aggregations);
            this.functions = ImmutableMap.copyOf(functions);
        }

        public Map<Symbol, Symbol> getDistinctAggregateSymbols()
        {
            return distinctAggregateSymbols;
        }

        public void setDistinctAggregateSymbols(Map<Symbol, Symbol> distinctAggregateSymbols)
        {
            this.distinctAggregateSymbols = distinctAggregateSymbols;
        }

        public Map<Symbol, Symbol> getNonDistinctAggregateSymbols()
        {
            return nonDistinctAggregateSymbols;
        }

        public void setNonDistinctAggregateSymbols(Map<Symbol, Symbol> nonDistinctAggregateSymbols)
        {
            this.nonDistinctAggregateSymbols = nonDistinctAggregateSymbols;
        }

        public Optional<Symbol> getMask()
        {
            return mask;
        }

        public List<Symbol> getGroupBySymbols()
        {
            return groupBySymbols;
        }

        public Map<Symbol, FunctionCall> getAggregations()
        {
            return aggregations;
        }

        public Map<Symbol, Signature> getFunctions()
        {
            return functions;
        }
    }
}
