package io.activej.launchers.dataflow.jdbc;

import io.activej.config.Config;
import io.activej.dataflow.calcite.CalciteSqlDataflow;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.dataflow.calcite.jdbc.DataflowMeta;
import io.activej.dataflow.calcite.jdbc.JdbcServerService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.dataflow.DataflowClientModule;
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

import java.time.Duration;
import java.util.List;

import static io.activej.config.converter.ConfigConverters.ofDuration;
import static io.activej.config.converter.ConfigConverters.ofInteger;

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
		Integer port = config.get(ofInteger(), "dataflow.jdbc.server.port");
		Duration idleTimeout = config.get(ofDuration(), "dataflow.jdbc.server.idleTimeout");

		return new Builder<Server>()
				.withHandler(handler)
				.withPort(port)
				.withServerCustomizers(List.of(server -> {
					Connector[] connectors = server.getConnectors();
					assert connectors.length == 1;
					((ServerConnector) connectors[0]).setIdleTimeout(idleTimeout.toMillis());
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
	Meta meta(Eventloop eventloop, CalciteSqlDataflow calciteSqlDataflow) {
		return DataflowMeta.create(eventloop, calciteSqlDataflow);
	}
}
