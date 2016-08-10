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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
public class ExpandNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Map<Symbol, Expression>> assignmentsList;
    private final List<Symbol> outputs;

    @JsonCreator
    public ExpandNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("assignmentsList") List<Map<Symbol, Expression>> assignmentsList)
    {
        super(id);

        this.source = source;
        this.assignmentsList = ImmutableList.copyOf(assignmentsList);
        this.outputs = ImmutableList.copyOf(assignmentsList.get(0).keySet());
        // TODO stagra: add assertion that all keys in list entries are same
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty
    public List<Map<Symbol, Expression>> getAssignmentsList()
    {
        return assignmentsList;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitExpand(this, context);
    }
}
