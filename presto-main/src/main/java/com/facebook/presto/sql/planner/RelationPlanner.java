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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.EquiJoinClause;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Values;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.analyzer.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final ConnectorSession session;

    RelationPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, ConnectorSession session)
    {
        Preconditions.checkNotNull(analysis, "analysis is null");
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");
        Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);
            return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), subPlan.getOutputSymbols());
        }

        TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : descriptor.getAllFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field.getName().get(), field.getType());

            outputSymbolsBuilder.add(symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        List<Symbol> planOutputSymbols = outputSymbolsBuilder.build();
        Optional<ColumnHandle> sampleWeightColumn = metadata.getSampleWeightColumnHandle(handle);
        Symbol sampleWeightSymbol = null;
        if (sampleWeightColumn.isPresent()) {
            sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BIGINT);
            outputSymbolsBuilder.add(sampleWeightSymbol);
            columns.put(sampleWeightSymbol, sampleWeightColumn.get());
        }

        List<Symbol> nodeOutputSymbols = outputSymbolsBuilder.build();
        PlanNode root = new TableScanNode(idAllocator.getNextId(), handle, nodeOutputSymbols, columns.build(), null, Optional.<GeneratedPartitions>absent());
        if (sampleWeightSymbol != null) {
            root = new MaterializeSampleNode(idAllocator.getNextId(), root, sampleWeightSymbol);
        }
        return new RelationPlan(root, descriptor, planOutputSymbols);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);

        return new RelationPlan(subPlan.getRoot(), outputDescriptor, subPlan.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        if (node.getColumnsToStratifyOn().isPresent()) {
            throw new UnsupportedOperationException("STRATIFY ON is not yet implemented");
        }

        RelationPlan subPlan = process(node.getRelation(), context);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);
        double ratio = analysis.getSampleRatio(node);
        Symbol sampleWeightSymbol = null;
        if (node.getType() == SampledRelation.Type.POISSONIZED) {
            sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BigintType.BIGINT);
        }
        PlanNode planNode = new SampleNode(idAllocator.getNextId(), subPlan.getRoot(), ratio, SampleNode.Type.fromType(node.getType()), node.isRescaled(), Optional.fromNullable(sampleWeightSymbol));
        if (sampleWeightSymbol != null) {
            planNode = new MaterializeSampleNode(idAllocator.getNextId(), planNode, sampleWeightSymbol);
        }
        return new RelationPlan(planNode, outputDescriptor, subPlan.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);
        RelationPlan rightPlan = process(node.getRight(), context);

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);

        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getOutputSymbols())
                .addAll(rightPlan.getOutputSymbols())
                .build();

        if (node.getType() == Join.Type.CROSS) {
            return new RelationPlan(
                    new JoinNode(idAllocator.getNextId(),
                            JoinNode.Type.typeConvert(node.getType()),
                            leftPlanBuilder.getRoot(),
                            rightPlanBuilder.getRoot(),
                            ImmutableList.<JoinNode.EquiJoinClause>of()),
                    outputDescriptor,
                    outputSymbols);
        }

        List<EquiJoinClause> criteria = analysis.getJoinCriteria(node);
        Analysis.JoinInPredicates joinInPredicates = analysis.getJoinInPredicates(node);

        // Add semi joins if necessary
        if (joinInPredicates != null) {
            leftPlanBuilder = appendSemiJoins(leftPlanBuilder, joinInPredicates.getLeftInPredicates());
            rightPlanBuilder = appendSemiJoins(rightPlanBuilder, joinInPredicates.getRightInPredicates());
        }

        // Add projections for join criteria
        leftPlanBuilder = appendProjections(leftPlanBuilder, Iterables.transform(criteria, leftGetter()));
        rightPlanBuilder = appendProjections(rightPlanBuilder, Iterables.transform(criteria, rightGetter()));

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();
        for (EquiJoinClause clause : criteria) {
            Symbol leftSymbol = leftPlanBuilder.translate(clause.getLeft());
            Symbol rightSymbol = rightPlanBuilder.translate(clause.getRight());

            clauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
        }

        return new RelationPlan(
                new JoinNode(idAllocator.getNextId(),
                        JoinNode.Type.typeConvert(node.getType()),
                        leftPlanBuilder.getRoot(),
                        rightPlanBuilder.getRoot(),
                        clauses.build()),
                outputDescriptor,
                outputSymbols);
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
            outputSymbols.add(subPlan.translate(fieldOrExpression));
        }

        return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), outputSymbols.build());
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
            outputSymbols.add(subPlan.translate(fieldOrExpression));
        }

        return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), outputSymbols.build());
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : descriptor.getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }

        ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
        for (Row row : node.getRows()) {
            ImmutableList.Builder<Expression> values = ImmutableList.builder();
            for (Expression expression : row.getItems()) {
                values.add(evaluateConstantExpression(expression));
            }
            rows.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbolsBuilder.build(), rows.build());
        return new RelationPlan(valuesNode, descriptor, outputSymbolsBuilder.build());
    }

    private Expression evaluateConstantExpression(final Expression expression)
    {
        try {
            // expressionInterpreter/optimizer only understands a subset of expression types
            // TODO: remove this when the new expression tree is implemented
            Expression canonicalized = CanonicalizeExpressions.canonicalizeExpression(expression);

            // verify the expression is constant (has no inputs)
            ExpressionInterpreter.expressionOptimizer(canonicalized, metadata, session, analysis.getTypes()).optimize(new SymbolResolver() {
                @Override
                public Object getValue(Symbol symbol)
                {
                    throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
                }
            });

            // evaluate the expression
            Object result = ExpressionInterpreter.expressionInterpreter(canonicalized, metadata, session, analysis.getTypes()).evaluate(0);
            checkState(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");

            return LiteralInterpreter.toExpression(result, analysis.getType(expression));
        }
        catch (Exception e) {
            throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Error evaluating constant expression: %s", e.getMessage());
        }
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        List<Symbol> unionOutputSymbols = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
        for (Relation relation : node.getRelations()) {
            RelationPlan relationPlan = process(relation, context);

            List<Symbol> childOutputSymobls = relationPlan.getOutputSymbols();
            if (unionOutputSymbols == null) {
                // Use the first Relation to derive output symbol names
                TupleDescriptor descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    Symbol symbol = childOutputSymobls.get(fieldIndex);
                    outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
                }
                unionOutputSymbols = outputSymbolBuilder.build();
            }

            TupleDescriptor descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == unionOutputSymbols.size(),
                    "Expected relation to have %s symbols but has %s symbols",
                    descriptor.getVisibleFieldCount(),
                    unionOutputSymbols.size());

            int unionFieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                symbolMapping.put(unionOutputSymbols.get(unionFieldId), childOutputSymobls.get(fieldIndex));
                unionFieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), sources.build(), symbolMapping.build());
        if (node.isDistinct()) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getOutputDescriptor(node), planNode.getOutputSymbols());
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getOutputSymbols());

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    private PlanBuilder appendProjections(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        // Carry over the translations from the source because we are appending projections
        translations.copyMappingsFrom(subPlan.getTranslations());

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        // add an identity projection for underlying plan
        for (Symbol symbol : subPlan.getRoot().getOutputSymbols()) {
            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
            projections.put(symbol, expression);
        }

        ImmutableMap.Builder<Symbol, Expression> newTranslations = ImmutableMap.builder();
        for (Expression expression : expressions) {
            Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));

            // TODO: CHECK IF THE REWRITE OF A SEMI JOINED EXPRESSION WILL WORK!!!!!!!

            projections.put(symbol, translations.rewrite(expression));
            newTranslations.put(symbol, expression);
        }
        // Now append the new translations into the TranslationMap
        for (Map.Entry<Symbol, Expression> entry : newTranslations.build().entrySet()) {
            translations.put(entry.getValue(), entry.getKey());
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()));
    }

    private PlanBuilder appendSemiJoins(PlanBuilder subPlan, Set<InPredicate> inPredicates)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendSemiJoin(subPlan, inPredicate);
        }
        return subPlan;
    }

    private PlanBuilder appendSemiJoin(PlanBuilder subPlan, InPredicate inPredicate)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        translations.copyMappingsFrom(subPlan.getTranslations());

        subPlan = appendProjections(subPlan, ImmutableList.of(inPredicate.getValue()));
        Symbol sourceJoinSymbol = subPlan.translate(inPredicate.getValue());

        Preconditions.checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        SubqueryExpression subqueryExpression = (SubqueryExpression) inPredicate.getValueList();
        RelationPlanner relationPlanner = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session);
        RelationPlan valueListRelation = relationPlanner.process(subqueryExpression.getQuery(), null);
        Symbol filteringSourceJoinSymbol = Iterables.getOnlyElement(valueListRelation.getRoot().getOutputSymbols());

        Symbol semiJoinOutputSymbol = symbolAllocator.newSymbol("semijoinresult", BOOLEAN);

        translations.put(inPredicate, semiJoinOutputSymbol);

        return new PlanBuilder(translations,
                new SemiJoinNode(idAllocator.getNextId(),
                        subPlan.getRoot(),
                        valueListRelation.getRoot(),
                        sourceJoinSymbol,
                        filteringSourceJoinSymbol,
                        semiJoinOutputSymbol));
    }

    private PlanNode distinct(PlanNode node)
    {
        return new AggregationNode(idAllocator.getNextId(),
                node,
                node.getOutputSymbols(),
                ImmutableMap.<Symbol, FunctionCall>of(),
                ImmutableMap.<Symbol, Signature>of(),
                ImmutableMap.<Symbol, Symbol>of(),
                Optional.<Symbol>absent(),
                1.0);
    }
}
