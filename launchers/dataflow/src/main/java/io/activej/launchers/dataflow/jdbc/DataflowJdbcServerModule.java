package io.activej.launchers.dataflow.jdbc;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverters;
import io.activej.dataflow.calcite.SqlDataflow;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.dataflow.calcite.jdbc.AvaticaJdbcServlet;
import io.activej.dataflow.calcite.jdbc.DataflowMeta;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpServer;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.dataflow.DataflowClientModule;
import io.activej.launchers.initializers.Initializers;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;

import java.util.concurrent.Executor;

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
	HttpServer server(NioReactor reactor, AsyncServlet servlet, Config config) {
		return HttpServer.builder(reactor, servlet)
				.initialize(Initializers.ofHttpServer(config.getChild("dataflow.jdbc.server")))
				.build();
	}

	@Provides
	AsyncServlet jdbcServlet(Executor executor, JsonService jsonService) {
		return AvaticaJdbcServlet.create(executor, jsonService);
	}

	@Provides
	Executor executor(Config config) {
		return ConfigConverters.getExecutor(config);
	}

	@Provides
	JsonService jsonService(Service service) {
		return new LocalJsonService(service);
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
