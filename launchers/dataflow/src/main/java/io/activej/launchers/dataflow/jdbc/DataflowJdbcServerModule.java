package io.activej.launchers.dataflow.jdbc;

import io.activej.config.Config;
import io.activej.dataflow.calcite.SqlDataflow;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.dataflow.calcite.jdbc.DataflowMeta;
import io.activej.dataflow.calcite.jdbc.JdbcServerService;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.dataflow.DataflowClientModule;
import io.activej.reactor.Reactor;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.HttpServer.Builder;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import static io.activej.config.converter.ConfigConverters.ofDuration;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;

public final class DataflowJdbcServerModule extends AbstractModule {
	private DataflowJdbcServerModule() {
	}

	public static DataflowJdbcServerModule create() {
		return new DataflowJdbcServerModule();
	}

	@Override
	protected void configure() {
		install(CalciteClientModule.create());
		install(DataflowClientModule.create());
	}

	@Provides
	@Eager
	JdbcServerService jdbcServerService(HttpServer server) {
		return JdbcServerService.create(server);
	}

	@Provides
	HttpServer server(AvaticaHandler handler, Config config) {
		InetSocketAddress address = config.get(ofInetSocketAddress(), "dataflow.jdbc.server.listenAddress");
		Duration idleTimeout = config.get(ofDuration(), "dataflow.jdbc.server.idleTimeout");

		return new Builder<Server>()
				.withHandler(handler)
				.withServerCustomizers(List.of(server -> {
					Connector[] connectors = server.getConnectors();
					assert connectors.length == 1;
					ServerConnector connector = (ServerConnector) connectors[0];
					connector.setIdleTimeout(idleTimeout.toMillis());
					connector.setHost(address.getHostName());
					connector.setPort(address.getPort());
				}), Server.class)
				.build();
	}

	@Provides
	AvaticaHandler handler(Service service) {
		return new AvaticaJsonHandler(service);
	}

	@Provides
	Service service(Meta meta) {
		return new LocalService(meta);
	}

	@Provides
	Meta meta(Reactor reactor, SqlDataflow sqlDataflow) {
		return DataflowMeta.create(reactor, sqlDataflow);
	}
}
