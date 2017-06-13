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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoFilter;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoJoinNode;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoProject;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoRelNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.tools.Frameworks;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rel.rules.FilterJoinRule.TRUE_PREDICATE;

/**
 * Created by shubham on 07/03/17.
 */
public class CalcitePlannerAction implements Frameworks.PlannerAction<PlanNode>
{
    private final PlanNode originalPlan;
    private final Metadata metadata;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Session session;
    RelOptCluster cluster;

    public CalcitePlannerAction(PlanNode plan, Metadata metadata, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        this.originalPlan = plan;
        this.metadata = metadata;
        this.idAllocator = idAllocator;
        this.symbolAllocator = symbolAllocator;
        this.session = session;
    }

    @Override
    public PlanNode apply(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus schemaPlus)
    {
        RelOptPlanner planner = new VolcanoPlanner(); // TODO: move to PrestoVolcanoPlanner when Cost model in place
        RelOptQuery query = new RelOptQuery(planner);
        RexBuilder rexBuilder = cluster.getRexBuilder();
        this.cluster = query.createCluster(rexBuilder.getTypeFactory(), rexBuilder);

        // 1. Convert Presto plan to CalcitePlan
        PrestoToCalcitePlanConvertor.PrestoToCalcitePlanContext prestoToCalcitePlanContext = new PrestoToCalcitePlanConvertor.PrestoToCalcitePlanContext();
        RelNode calcitePlan = originalPlan.accept(new PrestoToCalcitePlanConvertor(metadata, symbolAllocator, session, cluster, relOptSchema), prestoToCalcitePlanContext);

        // 2. Apply Calcite Optimizations
        // Add tests for the rules added
        PrestoRelNode optimizedPlan = (PrestoRelNode) hepPlan(calcitePlan, new DefaultRelMetadataProvider(),
                ReduceExpressionsRule.PROJECT_INSTANCE,
                ReduceExpressionsRule.FILTER_INSTANCE,
                ReduceExpressionsRule.JOIN_INSTANCE,
                ProjectRemoveRule.INSTANCE,
                new FilterProjectTransposeRule(Filter.class,
                        PrestoFilter.DEFAULT_FILTER_FACTORY,
                        PrestoProject.class,
                        PrestoProject.DEFAULT_PROJECT_FACTORY),
                new FilterMergeRule(PrestoFilter.DEFAULT_FILTER_FACTORY),
                new FilterJoinRule.JoinConditionPushRule(PrestoFilter.DEFAULT_FILTER_FACTORY,
                        PrestoProject.DEFAULT_PROJECT_FACTORY,
                        TRUE_PREDICATE),
                new FilterJoinRule.FilterIntoJoinRule(true,
                        PrestoFilter.DEFAULT_FILTER_FACTORY,
                        PrestoProject.DEFAULT_PROJECT_FACTORY,
                        TRUE_PREDICATE)
                );

        // 3. Apply join reordering
        /*List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(mdProvider.getMetadataProvider());*/
        RelTraitSet desiredTraits = cluster
                .traitSetOf(PrestoRelNode.CONVENTION, RelCollations.EMPTY);

        HepProgram hepPgm;
        HepProgramBuilder hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP)
                .addRuleInstance(new JoinToMultiJoinRule(PrestoJoinNode.class));
        hepPgmBldr.addRuleInstance(new LoptOptimizeJoinRule(PrestoJoinNode.PRESTO_JOIN_FACTORY,
                PrestoProject.DEFAULT_PROJECT_FACTORY, PrestoFilter.DEFAULT_FILTER_FACTORY));

        hepPgmBldr.addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE);
        hepPgmBldr.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
        hepPgmBldr.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
        hepPgmBldr.addRuleInstance(ProjectRemoveRule.INSTANCE);
        hepPgmBldr.addRuleInstance(UnionMergeRule.INSTANCE);
        hepPgmBldr.addRuleInstance(new ProjectMergeRule(false, PrestoProject.DEFAULT_PROJECT_FACTORY));

        hepPgm = hepPgmBldr.build();
        HepPlanner hepPlanner = new HepPlanner(hepPgm);

        //hepPlanner.registerMetadataProviders(ImmutableList.of(cluster.getMetadataProvider()));
        //RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
        //cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));

        RelNode rootRel = optimizedPlan;
        hepPlanner.setRoot(rootRel);
        if (!optimizedPlan.getTraitSet().equals(desiredTraits)) {
            rootRel = hepPlanner.changeTraits(optimizedPlan, desiredTraits);
        }
        hepPlanner.setRoot(rootRel);

        optimizedPlan = (PrestoRelNode) hepPlanner.findBestExp();

        // 3. Convert optimized Calcite plan to Presto Plan
        CalciteToPrestoPlanConverter calciteToPrestoPlanConverter = new CalciteToPrestoPlanConverter(idAllocator, symbolAllocator, metadata.getTypeManager());
        PlanNode convertedPlan = optimizedPlan.accept(calciteToPrestoPlanConverter, new CalciteToPrestoPlanConverter.Context());

        return addOutputNode(convertedPlan);
    }

    private PlanNode addOutputNode(PlanNode planNode)
    {
        // TODO validate the assumption that OutputNode's columnNames can be reused here
        OutputNode originalNode = (OutputNode) originalPlan;

        return new OutputNode(idAllocator.getNextId(),
                planNode,
                originalNode.getColumnNames(),
                planNode.getOutputSymbols());
    }

    // This is almost a copy of CalcitePlanner.hepPlan
    private RelNode hepPlan(RelNode basePlan, RelMetadataProvider mdProvider, RelOptRule... rules)
    {
        RelNode optimizedRelNode = basePlan;
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        programBuilder = programBuilder.addRuleCollection(ImmutableList.copyOf(rules));

        HepPlanner planner = new HepPlanner(programBuilder.build());
        List<RelMetadataProvider> list = new ArrayList();
        list.add(mdProvider);
        planner.registerMetadataProviders(list);
        RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
        basePlan.getCluster().setMetadataProvider(
                new CachingRelMetadataProvider(chainedProvider, planner));

        final RexExecutorImpl executor =
                new RexExecutorImpl(Schemas.createDataContext(null));
        basePlan.getCluster().getPlanner().setExecutor(executor);

        planner.setRoot(basePlan);
        optimizedRelNode = planner.findBestExp();

        return optimizedRelNode;
    }
}
