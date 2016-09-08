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
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.MoreObjects;

import java.util.List;

/**
 * Created by qubole on 7/9/16.
 */
public class GroupIdMatcher
    implements Matcher
{
    private final List<List<Symbol>> groups;

    public GroupIdMatcher(List<List<Symbol>> groups)
    {
        this.groups = groups;
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        if (!(node instanceof GroupIdNode)) {
            return false;
        }

        GroupIdNode groudIdNode = (GroupIdNode) node;
        List<List<Symbol>> actualGroups = groudIdNode.getGroupingSets();

        if (actualGroups.size() != groups.size()) {
            return false;
        }

        for (int i = 0; i < actualGroups.size(); i++) {
            actualGroups.get(i).removeAll(groups.get(i));
            if (!actualGroups.get(i).isEmpty()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("groups", groups)
                .toString();
    }
}
