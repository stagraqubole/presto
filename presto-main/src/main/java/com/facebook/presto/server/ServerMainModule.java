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
package com.facebook.presto.server;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.snappy.SnappyBlockEncoding;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.informationSchema.InformationSchemaModule;
import com.facebook.presto.connector.jmx.JmxConnectorFactory;
import com.facebook.presto.connector.system.SystemTablesModule;
import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryCreatedEvent;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.event.query.SplitCompletionEvent;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.TaskExecutor;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.CatalogManagerConfig;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientConfig;
import com.facebook.presto.operator.ExchangeClientFactory;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.operator.RecordSinkProvider;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.FixedWidthBlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.Serialization.FunctionCallDeserializer;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.slice.Slice;

import javax.inject.Singleton;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ServerMainModule
        extends AbstractConfigurationAwareModule
{
    private final SqlParserOptions sqlParserOptions;

    public ServerMainModule(SqlParserOptions sqlParserOptions)
    {
        this.sqlParserOptions = checkNotNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        // TODO: this should only be installed if this is a coordinator
        binder.install(new CoordinatorModule());

        if (serverConfig.isCoordinator()) {
            discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");
        }

        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);

        bindFailureDetector(binder, serverConfig.isCoordinator());

        // task execution
        jaxrsBinder(binder).bind(TaskResource.class);
        binder.bind(TaskManager.class).to(SqlTaskManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskManager.class).withGeneratedName();
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();
        binder.bind(LocalExecutionPlanner.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(CompilerConfig.class);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExpressionCompiler.class).withGeneratedName();
        bindConfig(binder).to(TaskManagerConfig.class);
        binder.bind(IndexJoinLookupStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IndexJoinLookupStats.class).withGeneratedName();

        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jaxrsBinder(binder).bind(PagesResponseWriter.class);

        // exchange client
        binder.bind(new TypeLiteral<Supplier<ExchangeClient>>() {}).to(ExchangeClientFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("exchange", ForExchange.class).withTracing();
        bindConfig(binder).to(ExchangeClientConfig.class);

        // execution
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class).withTracing();

        // data stream provider
        binder.bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorPageSourceProvider.class);

        // record sink provider
        binder.bind(RecordSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(RecordSinkProvider.class).to(RecordSinkManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorRecordSinkProvider.class);

        // metadata
        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(CatalogManagerConfig.class);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);

        // type
        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());

        // connector
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);
        MapBinder<String, ConnectorFactory> connectorFactoryBinder = newMapBinder(binder, String.class, ConnectorFactory.class);

        // jmx connector
        connectorFactoryBinder.addBinding("jmx").to(JmxConnectorFactory.class);

        // information schema
        binder.install(new InformationSchemaModule());

        // system tables
        binder.install(new SystemTablesModule());

        // splits
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);

        // query monitor
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);
        eventBinder(binder).bindEventClient(QueryCreatedEvent.class);
        eventBinder(binder).bindEventClient(QueryCompletionEvent.class);
        eventBinder(binder).bindEventClient(SplitCompletionEvent.class);

        // Determine the NodeVersion
        String prestoVersion = serverConfig.getPrestoVersion();
        if (prestoVersion == null) {
            prestoVersion = detectPrestoVersion();
        }
        checkState(prestoVersion != null, "presto.version must be provided when it cannot be automatically determined");

        NodeVersion nodeVersion = new NodeVersion(prestoVersion);
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        // presto announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto")
                .addProperty("node_version", nodeVersion.toString())
                .addProperty("datasources", nullToEmpty(serverConfig.getDataSources()));

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        jaxrsBinder(binder).bind(StatementResource.class);

        // execute resource
        jaxrsBinder(binder).bind(ExecuteResource.class);
        httpClientBinder(binder).bindHttpClient("execute", ForExecute.class);

        // plugin manager
        binder.bind(PluginManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(PluginManagerConfig.class);

        // optimizers
        binder.bind(new TypeLiteral<List<PlanOptimizer>>() {}).toProvider(PlanOptimizersFactory.class).in(Scopes.SINGLETON);

        // block encodings
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        Multibinder<BlockEncodingFactory<?>> blockEncodingFactoryBinder = newSetBinder(binder, new TypeLiteral<BlockEncodingFactory<?>>() {});
        blockEncodingFactoryBinder.addBinding().toInstance(VariableWidthBlockEncoding.FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(FixedWidthBlockEncoding.FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(RunLengthBlockEncoding.FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(DictionaryBlockEncoding.FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(SnappyBlockEncoding.FACTORY);

        // thread visualizer
        jaxrsBinder(binder).bind(ThreadResource.class);
    }

    @Provides
    @Singleton
    @ForExchange
    public ScheduledExecutorService createExchangeExecutor()
    {
        return newScheduledThreadPool(4, daemonThreadsNamed("exchange-client-%s"));
    }

    @Provides
    @Singleton
    @ForAsyncHttpResponse
    public static ScheduledExecutorService createAsyncHttpResponseExecutor()
    {
        return newScheduledThreadPool(10, daemonThreadsNamed("async-http-response-%s"));
    }

    private static String detectPrestoVersion()
    {
        String title = PrestoServer.class.getPackage().getImplementationTitle();
        String version = PrestoServer.class.getPackage().getImplementationVersion();
        return ((title == null) || (version == null)) ? null : (title + ":" + version);
    }

    private static void bindFailureDetector(Binder binder, boolean coordinator)
    {
        // TODO: this is a hack until the coordinator module works correctly
        if (coordinator) {
            binder.install(new FailureDetectorModule());
            jaxrsBinder(binder).bind(NodeResource.class);
        }
        else {
            binder.bind(FailureDetector.class).toInstance(new FailureDetector()
            {
                @Override
                public Set<ServiceDescriptor> getFailed()
                {
                    return ImmutableSet.of();
                }
            });
        }
    }
}
