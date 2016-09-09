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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestExtractDistinctAggregationOptimizer
{
    private final LocalQueryRunner queryRunner;

    public TestExtractDistinctAggregationOptimizer()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        defaultSession = defaultSession.withSystemProperty(SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true");

        this.queryRunner = new LocalQueryRunner(defaultSession);
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testExtractDistinctAggregationOptimizer()
    {
        @Language("SQL") String sql = "SELECT custkey, max(totalprice) AS s, Count(DISTINCT orderdate) AS d FROM orders GROUP BY custkey";
        Symbol group = new Symbol("group");
        // Original keys
        Symbol groupBy = new Symbol("custkey");
        Symbol aggregate = new Symbol("totalprice");
        Symbol distinctAggregation = new Symbol("orderdate");

        // Second Aggregation data
        List<Symbol> groupByKeysSecond = ImmutableList.of(groupBy);
        List<FunctionCall> aggregationsSecond = ImmutableList.of(
                functionCall("arbitrary", "*"),
                functionCall("count", "*"));

        // First Aggregation data
        List<Symbol> groupByKeysFirst = ImmutableList.of(groupBy, distinctAggregation, group);
        List<FunctionCall> aggregationsFirst = ImmutableList.of(functionCall("max", "totalprice"));

        // GroupingSet symbols
        ImmutableList.Builder<List<Symbol>> groups = ImmutableList.builder();
        groups.add(ImmutableList.of(groupBy, aggregate));
        groups.add(ImmutableList.of(groupBy, distinctAggregation));
        PlanMatchPattern expectedPlanPattern = anyTree(
                aggregation(ImmutableList.of(groupByKeysSecond), aggregationsSecond, ImmutableMap.of(), Optional.empty(),
                        project(
                                aggregation(ImmutableList.of(groupByKeysFirst), aggregationsFirst, ImmutableMap.of(), Optional.empty(),
                                        groupingSet(groups.build(),
                                                anyTree())))));

        List<PlanOptimizer> optimizerProvider = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new ExtractDistinctAggregationOptimizer(queryRunner.getMetadata()),
                new PruneUnreferencedOutputs());
        Plan actualPlan = queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, new FeaturesConfig(), optimizerProvider));

        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, expectedPlanPattern);
            return null;
        });
    }
}
