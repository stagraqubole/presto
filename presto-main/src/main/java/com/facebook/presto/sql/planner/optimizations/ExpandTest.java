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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.ExpandNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by qubole on 2/8/16.
 */
public class ExpandTest
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Optimizer(symbolAllocator, idAllocator), plan, Optional.empty());
    }

    private static class Optimizer
            extends SimplePlanRewriter<Optional<Symbol>>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;

        public Optimizer(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
        {
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Optional<Symbol>> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return context.defaultRewrite(node, Optional.<Symbol>empty());
            }

            TableScanNode tbsNode = (TableScanNode) node.getSource();
            Symbol groupSymbol = symbolAllocator.newSymbol("group", BigintType.BIGINT); // g
            List<Map<Symbol, Expression>> assignmentsList = new ArrayList<>();
            List<Symbol> tbsSymbols = tbsNode.getOutputSymbols();

            {
                ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
                for (int i = 0; i < tbsSymbols.size(); i++) {
                    if (i < 2) {
                        assignments.put(tbsSymbols.get(i), tbsSymbols.get(i).toQualifiedNameReference());
                    }
                    else {
                        assignments.put(tbsSymbols.get(i), new NullLiteral());
                    }
                }
                assignments.put(groupSymbol, new LongLiteral("0"));
                assignmentsList.add(assignments.build());
            }

            {
                ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
                for (int i = 0; i < tbsSymbols.size(); i++) {
                    if (i >= 2) {
                        assignments.put(tbsSymbols.get(i), tbsSymbols.get(i).toQualifiedNameReference());
                    }
                    else {
                        assignments.put(tbsSymbols.get(i), new NullLiteral());
                    }
                }
                assignments.put(groupSymbol, new LongLiteral("1"));
                assignmentsList.add(assignments.build());
            }

            ExpandNode expandNode = new ExpandNode(idAllocator.getNextId(),
                    context.rewrite(node.getSource(), Optional.empty()),
                    assignmentsList);

            List<String> colNames = new ArrayList<>();
            colNames.addAll(node.getColumnNames());
            colNames.add("group");
            OutputNode newNode = new OutputNode(node.getId(), expandNode, colNames, expandNode.getOutputSymbols());
            return newNode;
        }
    }
}
