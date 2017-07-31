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
package com.facebook.presto.sql.planner.optimizations.calcite.objects;

import com.facebook.presto.sql.planner.optimizations.calcite.PrestoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

/**
 * Created by shubham on 31/07/17.
 */
public class PrestoLimitNode
        extends SingleRel implements PrestoRelNode
{
    private final long limit;
    private final boolean partial;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    public PrestoLimitNode(RelOptCluster cluster, RelTraitSet traits, RelNode input, long limit, boolean partial)
    {
        super(cluster, traits, input);
        this.limit = limit;
        this.partial = partial;
    }

    public long getLimit()
    {
        return limit;
    }

    public boolean isPartial()
    {
        return partial;
    }

    @Override
    public PrestoLimitNode copy(RelTraitSet traitSet, List<RelNode> newInputs)
    {
        return new PrestoLimitNode(getCluster(), traitSet, sole(newInputs), limit, partial);
    }

    @Override
    public <C, R> R accept(PrestoRelVisitor<C, R> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }
}
