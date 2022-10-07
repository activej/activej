package io.activej.launchers.dataflow.jdbc;

import io.activej.config.Config;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.datastream.StreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.launchers.dataflow.DataflowClientLauncher;

import java.io.IOException;
import java.nio.file.Files;

public abstract class DataflowJdbcServerLauncher extends DataflowClientLauncher {
	public static final int DEFAULT_JDBC_SERVER_PORT = 3387;
	public static final String PROPERTIES_FILE = "dataflow-jdbc-server.properties";

	@Inject
	protected Eventloop eventloop;

	@Inject
	protected SqlDataflow sqlDataflow;

	/**
	 * Override this method to supply your dataflow schema.
	 * Bindings from this module override any other bindings.
	 */
	protected Module getDataflowSchemaModule() {
		return Module.empty();
	}

	/**
	 * Override this method to supply your own config.
	 */
	protected Config getConfig() {
		try {
			return Config.create()
					.with("dataflow.jdbc.server.port", String.valueOf(DEFAULT_JDBC_SERVER_PORT))
					.with("dataflow.secondaryBufferPath", Files.createTempDirectory("secondaryBufferPath").toString())
					.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
					.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected final Module getBusinessLogicModule() {
		return Modules.combine(
				CalciteClientModule.create(),
				DataflowJdbcServerModule.create()
		);
	}

	@Override
	protected final Module getOverrideModule() {
		return Modules.combine(
				getDataflowSchemaModule(),
				ModuleBuilder.create()
						.bind(Config.class).to(this::getConfig)
						.build()
		);
	}

	@Override
	protected final void onStart() throws Exception {
		eventloop.submit(() -> sqlDataflow.query("SELECT 1")
						.then(supplier -> supplier.streamTo(StreamConsumer.skip())))
				.get();

		logger.info("Connection to partitions established");
	}

	@Override
	protected final void run() throws Exception {
		awaitShutdown();
	}
}
