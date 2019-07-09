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

import io.prestosql.plugin.tpch.ColumnNaming;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.plugin.tpch.TpchNodePartitioningProvider;
import io.prestosql.plugin.tpch.TpchRecordSetProvider;
import io.prestosql.plugin.tpch.TpchSplitManager;
import io.prestosql.plugin.tpch.TpchTransactionHandle;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.Map;

public class ACIDTpchConnectorFactory
        extends TpchConnectorFactory
{
    @Override
    public String getName()
    {
        return "acid_tpch";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties, ConnectorContext context)
    {
        ColumnNaming columnNaming = ColumnNaming.valueOf(properties.getOrDefault(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()).toUpperCase());
        NodeManager nodeManager = context.getNodeManager();

        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return TpchTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new ACIDTpchMetadata();
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpchSplitManager(nodeManager, 1);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new TpchRecordSetProvider();
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                return new TpchNodePartitioningProvider(nodeManager, 1);
            }
        };
    }
}
