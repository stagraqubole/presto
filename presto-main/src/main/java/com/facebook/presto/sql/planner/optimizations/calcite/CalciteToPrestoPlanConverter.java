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
package com.facebook.presto.sql.planner.optimizations.calcite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.CalciteUnsupportedException;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.MalformedJoinException;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoFilter;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoJoinNode;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoProject;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoRelNode;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoTableScan;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by shubham on 12/04/17.
 */
public class CalciteToPrestoPlanConverter extends PrestoRelVisitor<CalciteToPrestoPlanConverter.Context, PlanNode>
{
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final TypeManager typeManager;
    private final Session session;
    private final Metadata metadata;

    /*
     * Each node visitor is responsible for setting context.symbols list for its parent node to use
     * Symbol at index 'i' in this list is:
     *  The Presto Symbol in PlanNode corresponding to the Calcite Symbol in the output of RelNode which is being converted to PlanNode
     * This list is later used in Parent to resolve RexInputReference
     */
    public CalciteToPrestoPlanConverter(Metadata metadata, Session session, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, TypeManager typeManager)
    {
        this.metadata = metadata;
        this.session = session;
        this.idAllocator = idAllocator;
        this.symbolAllocator = symbolAllocator;
        this.typeManager = typeManager;
    }

    @Override
    public PlanNode visitJoin(PrestoJoinNode node, Context context)
    {
        PlanNode left = ((PrestoRelNode) node.getLeft()).accept(this, context);
        RexNodeToExpressionConverter converter = new RexNodeToExpressionConverter(metadata, session, symbolAllocator, context.getSymbols());

        PlanNode right =  ((PrestoRelNode) node.getRight()).accept(this, context);
        converter.addSymbols(context.getSymbols());

        Expression joinCondition = converter.convert(node.getCondition());

        List<Expression> conditions = splitJoinConditions(joinCondition);
        List<JoinNode.EquiJoinClause> criterea = conditions
                .stream()
                .filter(condition -> (
                        (condition instanceof ComparisonExpression)
                                && (((ComparisonExpression) condition).getType() == ComparisonExpressionType.EQUAL)))
                .map(condition -> new JoinNode.EquiJoinClause(
                        Symbol.from(((ComparisonExpression) condition).getLeft()),
                        Symbol.from(((ComparisonExpression) condition).getRight()))
                )
                .collect(Collectors.toList());

        List<Expression> filters = conditions
                .stream()
                .filter(condition -> (
                        !((condition instanceof ComparisonExpression)
                                && (((ComparisonExpression) condition).getType() == ComparisonExpressionType.EQUAL))))
                .collect(Collectors.toList());
        checkState(filters.size() <= 1, "Join filter has more than one expression");

        JoinNode.Type joinType;
        switch (node.getJoinType()) {
            case RIGHT:
                joinType = JoinNode.Type.RIGHT;
                break;
            case INNER:
                joinType = JoinNode.Type.INNER;
                break;
            case LEFT:
                joinType = JoinNode.Type.LEFT;
                break;
            case FULL:
                joinType = JoinNode.Type.FULL;
                break;
            default:
                throw new CalciteUnsupportedException("Unknown join type in RelNode: " + node.getJoinType());
        }

        JoinNode joinNode = new JoinNode(
                idAllocator.getNextId(),
                joinType,
                left,
                right,
                criterea,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                filters.size() == 0 ? Optional.empty() : Optional.of(filters.get(0)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ImmutableList.Builder builder = ImmutableList.builder();
        for (Symbol symbol : joinNode.getOutputSymbols()) {
            builder.add(symbol);
        }
        context.setSymbols(builder.build());

        return fixJoinIfNeeded(joinNode);
    }

    private JoinNode fixJoinIfNeeded(JoinNode node)
    {
        // In case of join re-ordering, the returned condition is also reversed along with the table order
        // Because of this the left operand would be from right and vice versa, fixing that here
        ImmutableList.Builder critereaBuilder = ImmutableList.builder();
        for (JoinNode.EquiJoinClause equiJoinClause : node.getCriteria()) {
            Symbol left = equiJoinClause.getLeft();
            Symbol right = equiJoinClause.getRight();

            if (node.getLeft().getOutputSymbols().contains(right) &&
                    node.getRight().getOutputSymbols().contains(left)) {
                critereaBuilder.add(new JoinNode.EquiJoinClause(right, left));
            }
            else if (node.getLeft().getOutputSymbols().contains(left) &&
                    node.getRight().getOutputSymbols().contains(right)) {
                critereaBuilder.add(new JoinNode.EquiJoinClause(left, right));
            }
            else {
                throw new MalformedJoinException(String.format("[%s, %s] symbols not found in source nodes",
                        left, right));
            }
        }

        return new JoinNode(
                idAllocator.getNextId(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                critereaBuilder.build(),
                node.getOutputSymbols(),
                node.getFilter(),
                node.getLeftHashSymbol(),
                node.getRightHashSymbol(),
                node.getDistributionType());
    }

    @Override
    public PlanNode visitProject(PrestoProject node, Context context)
    {
        PlanNode source = ((PrestoRelNode) node.getInput()).accept(this, context);
        RexNodeToExpressionConverter converter = new RexNodeToExpressionConverter(metadata, session, symbolAllocator, context.getSymbols());

        // projectNode.outputSymbols is map.keyset so ordering might not match relNode's output field list
        ImmutableList.Builder relNodeToPlanNodeOutputMapping = new ImmutableList.Builder();
        Assignments.Builder assignments = Assignments.builder();

        for (int i = 0; i < node.getChildExps().size(); i++) {
            Expression prestoExpr = converter.convert(node.getChildExps().get(i));
            Symbol symbol = symbolAllocator.newSymbol(node.getRowType().getFieldList().get(i).getName(), TypeConverter.convert(typeManager, node.getChildExps().get(i).getType()));
            assignments.put(symbol, prestoExpr);
            relNodeToPlanNodeOutputMapping.add(symbol);
        }

        context.setSymbols(relNodeToPlanNodeOutputMapping.build());
        return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
    }

    @Override
    public PlanNode visitFilter(PrestoFilter node, Context context)
    {
        PlanNode source = ((PrestoRelNode) node.getInput()).accept(this, context);

        if (node.getCondition() instanceof RexCall) {
            RexNodeToExpressionConverter converter = new RexNodeToExpressionConverter(metadata, session, symbolAllocator, context.getSymbols());

            context.setSymbols(source.getOutputSymbols());
            return new FilterNode(idAllocator.getNextId(), source, converter.convert(node.getCondition()));
        }
        else {
            throw new UnsupportedOperationException("Unsupported type of condition " + node.getCondition().getClass());
        }
    }

    @Override
    public PlanNode visitTableScan(PrestoTableScan node, Context context)
    {
        TableScanNode planNode = node.getPlanNode();
        // We shouldnt have a case where Calcite changed the TableScanNode
        checkState(planNode.getAssignments().size() == node.getRowType().getFieldList().size(), "Number of rows have changed in Calcite TableScan from original Plan");
        List<RelDataTypeField> newFieldsInCalciteNode = (node.getRowType()).getFieldList().stream()
                .filter(field -> !planNode.getAssignments().keySet().contains(new Symbol(field.getName())))
                .collect(Collectors.toList());
        checkState(newFieldsInCalciteNode.size() == 0, "New fields in Calcite Plan: " + newFieldsInCalciteNode);
        context.setSymbols(planNode.getOutputSymbols());
        return planNode;
    }

    private List<Expression> splitJoinConditions(Expression expression)
    {
        if (!(expression instanceof LogicalBinaryExpression)) {
            return ImmutableList.of(expression);
        }

        LogicalBinaryExpression binaryExpression = (LogicalBinaryExpression) expression;
        if (binaryExpression.getType() != LogicalBinaryExpression.Type.AND) {
            return ImmutableList.of(expression);
        }

        ImmutableList.Builder builder = new ImmutableList.Builder();

        builder.addAll(splitJoinConditions(binaryExpression.getLeft()));
        builder.addAll(splitJoinConditions(binaryExpression.getRight()));
        return builder.build();
    }

    static class Context
    {
        // These symbols are only useful only to the parent of the node and each node sets this according to its output symbols
        private List<Symbol> symbols;

        public void setSymbols(List symbols)
        {
            this.symbols = symbols;
        }

        public List getSymbols()
        {
            return symbols;
        }
    }
}
