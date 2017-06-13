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

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Created by shubham on 23/03/17.
 */
public class RelOptPrestoTable extends RelOptAbstractTable
{
    double rowCount = 100D;

    public RelOptPrestoTable(RelOptSchema schema, String name, RelDataType rowType, double rowCount)
    {
        super(schema, name, rowType);
        this.rowCount = rowCount;
    }

    @Override
    public double getRowCount()
    {
        return rowCount;
    }
}
