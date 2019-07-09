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
package io.prestosql.sql.planner.acid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchMetadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.type.BooleanType;

import java.util.Map;

public class ACIDTpchMetadata
        extends TpchMetadata
{
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata actual = super.getTableMetadata(session, tableHandle);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.addAll(actual.getColumns());
        columns.add(new ColumnMetadata("$isValid", BooleanType.BOOLEAN, null, true));
        return new ConnectorTableMetadata(actual.getTable(), columns.build());
    }
}
