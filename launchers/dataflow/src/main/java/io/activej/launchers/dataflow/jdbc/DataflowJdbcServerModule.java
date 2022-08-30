package io.activej.launchers.dataflow.jdbc;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverters;
import io.activej.dataflow.calcite.CalciteSqlDataflow;
import io.activej.dataflow.calcite.jdbc.server.DataflowMeta;
import io.activej.dataflow.calcite.jdbc.server.JdbcServerService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;

public final class DataflowJdbcServerModule extends AbstractModule {
	private DataflowJdbcServerModule() {
	}

	public static DataflowJdbcServerModule create() {
		return new DataflowJdbcServerModule();
	}

	@Provides
	@Eager
	JdbcServerService jdbcServerService(HttpServer server) {
		return JdbcServerService.create(server);
	}

	@Provides
	HttpServer server(AvaticaHandler handler, Config config) {
		Integer port = config.get(ConfigConverters.ofInteger(), "dataflow.jdbc.server.port");

		return new HttpServer.Builder<>()
				.withHandler(handler)
				.withPort(port)
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
		return new DataflowMeta(eventloop, calciteSqlDataflow);
	}
}
