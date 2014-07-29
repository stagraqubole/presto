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
package com.facebook.presto.tests;

import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.testing.Assertions;
import io.airlift.testing.Closeables;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DistributedQueryRunner
        implements QueryRunner
{
    private static final String ENVIRONMENT = "testing";

    private final TestingDiscoveryServer discoveryServer;
    private final TestingPrestoServer coordinator;
    private final List<TestingPrestoServer> servers;

    private final TestingPrestoClient prestoClient;

    public DistributedQueryRunner(ConnectorSession defaultSession, int workersCount)
            throws Exception
    {
        checkNotNull(defaultSession, "defaultSession is null");

        try {
            discoveryServer = new TestingDiscoveryServer(ENVIRONMENT);

            ImmutableList.Builder<TestingPrestoServer> servers = ImmutableList.builder();
            coordinator = createTestingPrestoServer(discoveryServer.getBaseUrl(), true);
            servers.add(coordinator);

            for (int i = 1; i < workersCount; i++) {
                servers.add(createTestingPrestoServer(discoveryServer.getBaseUrl(), false));
            }
            this.servers = servers.build();
        }
        catch (Exception e) {
            close();
            throw e;
        }

        this.prestoClient = new TestingPrestoClient(coordinator, defaultSession);

        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            Assertions.assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }

        for (TestingPrestoServer server : servers) {
            server.getMetadata().addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
        }
    }

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator)
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("compiler.interpreter-enabled", "false")
                .put("datasources", "system");
        if (coordinator) {
            properties.put("node-scheduler.include-coordinator", "false");
        }

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties.build(), ENVIRONMENT, discoveryUri, ImmutableList.<Module>of());

        return server;
    }

    private boolean allNodesGloballyVisible()
    {
        for (TestingPrestoServer server : servers) {
            AllNodes allNodes = server.refreshNodes();
            if (!allNodes.getInactiveNodes().isEmpty() ||
                    (allNodes.getActiveNodes().size() != servers.size())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getNodeCount()
    {
        return servers.size();
    }

    @Override
    public ConnectorSession getDefaultSession()
    {
        return prestoClient.getDefaultSession();
    }

    public TestingPrestoServer getCoordinator()
    {
        return coordinator;
    }

    public void installPlugin(Plugin plugin)
    {
        for (TestingPrestoServer server : servers) {
            server.installPlugin(plugin);
        }
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.<String, String>of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        for (TestingPrestoServer server : servers) {
            server.createCatalog(catalogName, connectorName, properties);
        }

        // wait for all nodes to announce the new catalog
        long start = System.nanoTime();
        while (!isConnectionVisibleToAllNodes(catalogName)) {
            Assertions.assertLessThan(nanosSince(start), new Duration(100, SECONDS), "waiting form connector " + connectorName + " to be initialized in every node");
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

    private boolean isConnectionVisibleToAllNodes(String connectorId)
    {
        for (TestingPrestoServer server : servers) {
            server.refreshNodes();
            Set<Node> activeNodesWithConnector = server.getActiveNodesWithConnector(connectorId);
            if (activeNodesWithConnector.size() != servers.size()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<QualifiedTableName> listTables(ConnectorSession session, String catalog, String schema)
    {
        return prestoClient.listTables(session, catalog, schema);
    }

    @Override
    public boolean tableExists(ConnectorSession session, String table)
    {
        return prestoClient.tableExists(session, table);
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return prestoClient.execute(sql);
    }

    @Override
    public MaterializedResult execute(ConnectorSession session, @Language("SQL") String sql)
    {
        return prestoClient.execute(session, sql);
    }

    @Override
    public final void close()
    {
        if (servers != null) {
            for (TestingPrestoServer server : servers) {
                Closeables.closeQuietly(server);
            }
        }
        Closeables.closeQuietly(prestoClient);
        Closeables.closeQuietly(discoveryServer);
    }
}
