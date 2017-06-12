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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.optimizations.DesugaringOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

/**
 * Created by shubham on 27/04/17.
 */
public class TestFilterPushdown
{
    private final LocalQueryRunner queryRunner;
    private final SqlParser sqlParser;

    public TestFilterPushdown()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.ENABLE_CALCITE, "true")
                .build();

        this.queryRunner = new LocalQueryRunner(defaultSession);
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        sqlParser = new SqlParser();
    }

    @Test
    public void testFilterPushdownBelowProject()
    {
        String sql = "SELECT extendedprice FROM (SELECT receiptdate, shipdate, extendedprice FROM lineitem) a WHERE a.receiptdate = a.shipdate";

        PlanMatchPattern expectedPlanPattern =
                anyTree(
                        filter("receiptdate = shipdate",
                                tableScan("lineitem", ImmutableMap.of(
                                        "receiptdate", "receiptdate",
                                        "shipdate", "shipdate")
                                )
                        )
                );

        List<PlanOptimizer> optimizerProvider = ImmutableList.of(
                new DesugaringOptimizer(queryRunner.getMetadata(), sqlParser),
                new CalciteOptimizer(queryRunner.getMetadata()));

        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizerProvider);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getCostCalculator(), actualPlan, expectedPlanPattern);
            return null;
        });
    }

    @Test
    public void testFilterPushdownBelowJoin()
    {
        String sql = "SELECT receiptdate from orders join lineitem on lineitem.orderkey = orders.orderkey where receiptdate = shipdate";
        PlanMatchPattern expectedPlanPattern =
                anyTree(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("orderkey_o", "orderkey_l")),
                                any(
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey_o", "orderkey"
                                        ))
                                ),
                                anyTree(
                                        filter("receiptdate_l = shipdate_l",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "orderkey_l", "orderkey",
                                                        "receiptdate_l", "receiptdate",
                                                        "shipdate_l", "shipdate"))
                                        )
                                )
                        )
                );

        List<PlanOptimizer> optimizerProvider = ImmutableList.of(
                new DesugaringOptimizer(queryRunner.getMetadata(), sqlParser),
                new CalciteOptimizer(queryRunner.getMetadata()),
                new UnaliasSymbolReferences());

        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizerProvider);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getCostCalculator(), actualPlan, expectedPlanPattern);
            return null;
        });
    }
}
