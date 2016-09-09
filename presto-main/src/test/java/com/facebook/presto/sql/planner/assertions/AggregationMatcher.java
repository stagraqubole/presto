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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by qubole on 8/9/16.
 */
public class AggregationMatcher
        implements Matcher
{
    private final List<Symbol> groupByKeys;
    private final List<FunctionCall> aggregations;
    private final List<Symbol> masks;
    private final List<List<Symbol>> groupingSets;

    public AggregationMatcher(List<Symbol> groupByKeys, List<FunctionCall> aggregations, List<Symbol> masks, List<List<Symbol>> groupingSets)
    {
        this.groupByKeys = groupByKeys;
        this.aggregations = aggregations;
        this.masks = masks;
        this.groupingSets = groupingSets;
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        if (!(node instanceof AggregationNode)) {
            return false;
        }

        AggregationNode aggregationNode = (AggregationNode) node;

        boolean match =  matches(groupByKeys, aggregationNode.getGroupBy()) &&
                matches(masks, aggregationNode.getMasks().values().stream().collect(Collectors.toList()));

        if (!match) {
            return match;
        }

        for (int i = 0; i < groupingSets.size(); i++) {
            if (!matches(groupingSets.get(i), aggregationNode.getGroupingSets().get(i))) {
                return false;
            }
        }

        if (!matches(aggregations, aggregationNode.getAggregations().values().stream().collect(Collectors.toList()))) {
            return false;
        }

        return true;
    }

    private <T> boolean matches(List<T> expected, List<T> actual)
    {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (T symbol : expected) {
            if (!actual.contains(symbol)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("groupsByKeys", groupByKeys)
                .add("aggregations", aggregations)
                .add("masks", masks)
                .add("groupingSets", groupingSets)
                .toString();
    }
}
