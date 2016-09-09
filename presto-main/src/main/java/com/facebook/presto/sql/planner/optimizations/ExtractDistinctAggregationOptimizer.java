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
import com.facebook.presto.metadata.Metadata;
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
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

/*
 * This optimizer convert query of form:
 *  SELECT a1, a2,..., an, F1(b1), F2(b2), F3(b3), ...., Fm(bm), F(distinct c) FROM Table GROUP BY a1, a2, ..., an
 *
 *  INTO
 *  SELECT a1, a2,..., an, arbitrary(f1 if group = 0 else null),...., arbitrary(fm if group = 0 else null), F(c if group = 1 else null) FROM
 *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c, group FROM
 *        SELECT a1, a2,..., an, b1, b2, ... ,bn, c FROM Table GROUP BY GROUPING SETS ((a1, a2,..., an, b1, b2, ... ,bn), (a1, a2,..., an, c))
 *      GROUP BY a1, a2,..., an, c, group
 *  GROUP BY a1, a2,..., an
 */
public class ExtractDistinctAggregationOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public ExtractDistinctAggregationOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (SystemSessionProperties.isOptimizeDistinctAggregationEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Optimizer(idAllocator, symbolAllocator, metadata),
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
        private final Metadata metadata;

        private Optimizer(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
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

            AggregateInfo aggregateInfo = new AggregateInfo(node.getGroupBy(),
                    Iterables.getOnlyElement(masks),
                    node.getAggregations(),
                    node.getFunctions());

            PlanNode source = context.rewrite(node.getSource(), Optional.of(aggregateInfo));

            // Change aggregate node to do second aggregation, handles this part of optimized plan mentioned above:
            //          SELECT a1, a2,..., an, arbitrary(f1 if group = 0 else null),...., arbitrary(fm if group = 0 else null), F(c if group = 1 else null)
            ImmutableMap.Builder<Symbol, FunctionCall> aggregations = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                FunctionCall functionCall = entry.getValue();
                if (entry.getValue().isDistinct()) {
                    aggregations.put(entry.getKey(), new FunctionCall(functionCall.getName(),
                            functionCall.getWindow(),
                            false,
                            ImmutableList.of(Iterables.getOnlyElement(aggregateInfo.getNewDistinctAggregateSymbols().values()).toSymbolReference())));
                    functions.put(entry.getKey(), node.getFunctions().get(entry.getKey()));
                }
                else {
                    // Aggregations on non-distinct are already done by new node, just extract the non-null value
                    Symbol argument = aggregateInfo.getNewNonDistinctAggregateSymbols().get(entry.getKey());
                    QualifiedName functionName = QualifiedName.of("arbitrary");
                    aggregations.put(entry.getKey(), new FunctionCall(functionName,
                            functionCall.getWindow(),
                            false,
                            ImmutableList.of(argument.toSymbolReference())));
                    functions.put(entry.getKey(),
                            metadata.getFunctionRegistry().resolveFunction(functionName, ImmutableList.of(symbolAllocator.getTypes().get(argument).getTypeSignature()), false));
                }
            }

            return new AggregationNode(idAllocator.getNextId(),
                    source,
                    node.getGroupBy(),
                    aggregations.build(),
                    functions.build(),
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
            Optional<AggregateInfo> aggregateInfo = context.get();
            // presence of aggregateInfo => mask also present
            if (aggregateInfo.isPresent() && aggregateInfo.get().getMask().get().equals(node.getMarkerSymbol())) {
                PlanNode source = context.rewrite(node.getSource(), Optional.empty());

                List<Symbol> allSymbols = new ArrayList<>();
                List<Symbol> groupBySymbols = aggregateInfo.get().getGroupBySymbols(); // a
                List<Symbol> nonDistinctAggregateSymbols = aggregateInfo.get().getOriginalNonDistinctAggregateArgs(); //b
                Symbol distinctSymbol = Iterables.getOnlyElement(aggregateInfo.get().getOriginalDistinctAggregateArgs()); // c

                allSymbols.addAll(groupBySymbols);
                allSymbols.addAll(nonDistinctAggregateSymbols);
                allSymbols.add(distinctSymbol);

                // If same symbol present in aggregations on distinct and non-distinct values, e.g. select sum(a), count(distinct a),
                // then we need to create a duplicate stream for this symbol
                Symbol duplicatedDistinctSymbol = distinctSymbol;

                if (nonDistinctAggregateSymbols.contains(distinctSymbol)) {
                    Symbol newSymbol = symbolAllocator.newSymbol(distinctSymbol.getName(), symbolAllocator.getTypes().get(distinctSymbol));
                    nonDistinctAggregateSymbols.set(nonDistinctAggregateSymbols.indexOf(distinctSymbol), newSymbol);
                    duplicatedDistinctSymbol = newSymbol;

                    // Add project node to get stream for new symbol
                    ImmutableMap.Builder builder = new ImmutableMap.Builder();
                    for (Symbol symbol : node.getSource().getOutputSymbols()) {
                        builder.put(symbol, symbol.toSymbolReference());
                    }
                    // Cast to avoid UnaliasSymbolReference from removing this projection
                    // TODO: find better way to prevent UnaliaSymbolRefrence optimizing out the project
                    builder.put(newSymbol, new Cast(distinctSymbol.toSymbolReference(),
                            symbolAllocator.getTypes().get(distinctSymbol).getDisplayName()));

                    source = new ProjectNode(idAllocator.getNextId(), source, builder.build());
                }

                // 1. Add GroupIdNode
                Symbol groupSymbol = symbolAllocator.newSymbol("group", BigintType.BIGINT); // g
                GroupIdNode groupIdNode = createGroupIdNode(groupBySymbols,
                        nonDistinctAggregateSymbols,
                        distinctSymbol,
                        groupSymbol,
                        source);

                // 2. Add aggregation node
                List<Symbol> groupByKeys = new ArrayList<>();
                groupByKeys.addAll(groupBySymbols);
                groupByKeys.add(distinctSymbol);
                groupByKeys.add(groupSymbol);

                ImmutableMap.Builder aggregationOuputSymbolsMapBuilder = ImmutableMap.builder();
                AggregationNode aggregationNode = createNonDistinctAggregation(aggregateInfo.get(),
                        distinctSymbol,
                        duplicatedDistinctSymbol,
                        groupByKeys,
                        groupIdNode,
                        node,
                        aggregationOuputSymbolsMapBuilder);
                // This map has mapping only for aggregation on non-distinct symbols which the new AggregationNode handles
                Map<Symbol, Symbol> aggregationOuputSymbolsMap = aggregationOuputSymbolsMapBuilder.build();

                 // 3. Add new project node that adds if expressions
                ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbols = ImmutableMap.builder();
                ImmutableMap.Builder<Symbol, Symbol> outputDistinctAggregateSymbols = ImmutableMap.builder();
                ProjectNode projectNode = createProjectNode(aggregationNode,
                        aggregateInfo.get(),
                        distinctSymbol,
                        groupSymbol,
                        aggregationOuputSymbolsMap,
                        outputNonDistinctAggregateSymbols,
                        outputDistinctAggregateSymbols);
                aggregateInfo.get().setNewNonDistinctAggregateSymbols(outputNonDistinctAggregateSymbols.build());
                aggregateInfo.get().setNewDistinctAggregateSymbols(outputDistinctAggregateSymbols.build());
                return projectNode;
            }
            return context.defaultRewrite(node, Optional.empty());
        }

        /*
         * This Project is useful for cases when we aggregate on distinct and non-distinct values of same symbol, eg:
         *  select a, sum(b), count(c), sum(distinct c) group by a
         * Without this Project, we would count additional values for count(c)
         *
         * This method also populates maps of old to new symbols. For each key of outputNonDistinctAggregateSymbols,
         * Higher level aggregation node's aggregaton <key, AggregateExpression> will now have to run AggregateExpression on value of outputNonDistinctAggregateSymbols
         * Same for outputDistinctAggregateSymbols map
         */
        private ProjectNode createProjectNode(AggregationNode source,
                AggregateInfo aggregateInfo,
                Symbol distinctSymbol,
                Symbol groupSymbol,
                Map<Symbol, Symbol> aggregationOuputSymbolsMap,
                ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbols,
                ImmutableMap.Builder<Symbol, Symbol> outputDistinctAggregateSymbols
                )
        {
            ImmutableMap.Builder<Symbol, Expression> outputSymbols = ImmutableMap.builder();
            for (Symbol symbol : source.getOutputSymbols()) {
                if (distinctSymbol.equals(symbol)) {
                    Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                    outputDistinctAggregateSymbols.put(symbol, newSymbol);

                    Expression expression = createIfExpression(groupSymbol.toSymbolReference(),
                            new Cast(new LongLiteral("1"), "bigint"),
                            ComparisonExpression.Type.EQUAL,
                            symbol.toSymbolReference());
                    outputSymbols.put(newSymbol, expression);
                }
                else if (aggregationOuputSymbolsMap.containsKey(symbol)) {
                    Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                    outputNonDistinctAggregateSymbols.put(aggregationOuputSymbolsMap.get(symbol), newSymbol); // key of this map is key of an aggregation in AggrNode above, it will now aggregate on this Map's value
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
            outputSymbols.put(aggregateInfo.getMask().get(), new NullLiteral());

            return new ProjectNode(idAllocator.getNextId(),
                    source,
                    outputSymbols.build());
        }

        private GroupIdNode createGroupIdNode(List<Symbol> groupBySymbols,
                List<Symbol> nonDistinctAggregateSymbols,
                Symbol distinctSymbol,
                Symbol groupSymbol,
                PlanNode source)
        {
            List<List<Symbol>> groups = new ArrayList<>();
            // g0 = {allGBY_Symbols + allNonDistinctAggregateSymbols}
            // g1 = {allGBY_Symbols + Distinct Symbol}
            // symbols present in Group_i will be set, rest will be Null

            //g0
            List<Symbol> group0 = new ArrayList<>();
            group0.addAll(groupBySymbols);
            group0.addAll(nonDistinctAggregateSymbols);
            groups.add(group0);

            // g1
            List<Symbol> group1 = new ArrayList<>();
            group1.addAll(groupBySymbols);
            group1.add(distinctSymbol);
            groups.add(group1);

            return new GroupIdNode(idAllocator.getNextId(),
                    source,
                    groups,
                    ImmutableMap.of(),
                    groupSymbol);
        }
        /*
         * This method returns a new Aggregation node which has aggregations on non-distinct symbols from original plan. Generates
         *      SELECT a1, a2,..., an, F1(b1) as f1, F2(b2) as f2,...., Fm(bm) as fm, c, group
         * part in the optimized plan mentioned above
         *
         * It also populates the mappings of new function's output symbol to corresponding old function's output symbol, e.g.
         *     { f1 -> F1, f2 -> F2, ... }
         * The new AggregateNode aggregates on the symbols that original AggregationNode aggregated on
         * Original one will now aggregate on the output symbols of this new node
         */
        private AggregationNode createNonDistinctAggregation(AggregateInfo aggregateInfo,
                Symbol distinctSymbol,
                Symbol duplicatedDistinctSymbol,
                List<Symbol> groupByKeys,
                GroupIdNode groupIdNode,
                MarkDistinctNode originalNode,
                ImmutableMap.Builder aggregationOuputSymbolsMapBuilder
        )
        {
            ImmutableMap.Builder<Symbol, FunctionCall> aggregations = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : aggregateInfo.getAggregations().entrySet()) {
                FunctionCall functionCall = entry.getValue();
                if (!functionCall.isDistinct()) {
                    Symbol newSymbol = symbolAllocator.newSymbol(entry.getKey().toSymbolReference(), symbolAllocator.getTypes().get(entry.getKey()));
                    aggregationOuputSymbolsMapBuilder.put(newSymbol, entry.getKey());
                    if (duplicatedDistinctSymbol == distinctSymbol) {
                        // Mask symbol was not present in aggregations without mask
                        aggregations.put(newSymbol, functionCall);
                    }
                    else {
                        // Handling for cases when mask symbol appears in non distinct aggregations too
                        // Now the aggregation should happen over the duplicate symbol added before
                        if (functionCall.getArguments().contains(distinctSymbol.toSymbolReference())) {
                            ImmutableList.Builder arguments = ImmutableList.builder();
                            for (Expression argument : functionCall.getArguments()) {
                                if (distinctSymbol.toSymbolReference().equals(argument)) {
                                    arguments.add(duplicatedDistinctSymbol.toSymbolReference());
                                }
                                else {
                                    arguments.add(argument);
                                }
                            }
                            aggregations.put(newSymbol, new FunctionCall(functionCall.getName(), functionCall.getWindow(), false, arguments.build()));
                        }
                        else {
                            aggregations.put(newSymbol, functionCall);
                        }
                    }
                    functions.put(newSymbol, aggregateInfo.getFunctions().get(entry.getKey()));
                }
            }
            return new AggregationNode(idAllocator.getNextId(),
                    groupIdNode,
                    groupByKeys,
                    aggregations.build(),
                    functions.build(),
                    Collections.emptyMap(),
                    ImmutableList.of(groupByKeys),
                    SINGLE,
                    Optional.empty(),
                    1.0,
                    originalNode.getHashSymbol());
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
    }

    private static class AggregateInfo
    {
        private List<Symbol> groupBySymbols;
        private Optional<Symbol> mask = Optional.empty();
        private Map<Symbol, FunctionCall> aggregations;
        private Map<Symbol, Signature> functions;

        // Filled on the way back, these are the symbols corresponding to their distinct or non-distinct original symbols
        private Map<Symbol, Symbol> newNonDistinctAggregateSymbols;
        private Map<Symbol, Symbol> newDistinctAggregateSymbols;

        public AggregateInfo(List<Symbol> groupBySymbols, Symbol mask, Map<Symbol, FunctionCall> aggregations, Map<Symbol, Signature> functions)
        {
            this.groupBySymbols = ImmutableList.copyOf(groupBySymbols);

            if (mask != null) {
                this.mask = Optional.of(mask);
            }

            this.aggregations = ImmutableMap.copyOf(aggregations);
            this.functions = ImmutableMap.copyOf(functions);
        }

        public List<Symbol> getOriginalNonDistinctAggregateArgs()
        {
            Set<Symbol> arguments = new HashSet<Symbol>();
            aggregations.values()
                    .stream()
                    .filter(functionCall -> !functionCall.isDistinct())
                    .forEach(functionCall -> functionCall.getArguments().forEach(argument -> arguments.add(Symbol.from(argument))));
            return arguments.stream().collect(Collectors.toList());
        }

        public List<Symbol> getOriginalDistinctAggregateArgs()
        {
            Set<Symbol> arguments = new HashSet<Symbol>();
            aggregations.values()
                    .stream()
                    .filter(functionCall -> functionCall.isDistinct())
                    .forEach(functionCall -> functionCall.getArguments().forEach(argument -> arguments.add(Symbol.from(argument))));
            return arguments.stream().collect(Collectors.toList());
        }

        public Map<Symbol, Symbol> getNewDistinctAggregateSymbols()
        {
            return newDistinctAggregateSymbols;
        }

        public void setNewDistinctAggregateSymbols(Map<Symbol, Symbol> newDistinctAggregateSymbols)
        {
            this.newDistinctAggregateSymbols = newDistinctAggregateSymbols;
        }

        public Map<Symbol, Symbol> getNewNonDistinctAggregateSymbols()
        {
            return newNonDistinctAggregateSymbols;
        }

        public void setNewNonDistinctAggregateSymbols(Map<Symbol, Symbol> newNonDistinctAggregateSymbols)
        {
            this.newNonDistinctAggregateSymbols = newNonDistinctAggregateSymbols;
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
