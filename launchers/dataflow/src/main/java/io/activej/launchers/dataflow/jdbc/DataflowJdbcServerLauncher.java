package io.activej.launchers.dataflow.jdbc;

import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.dataflow.ISqlDataflow;
import io.activej.datastream.StreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.config.converter.ConfigConverters.ofDuration;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class DataflowJdbcServerLauncher extends Launcher {
	public static final String DEFAULT_JDBC_SERVER_HOSTNAME = "localhost";
	public static final int DEFAULT_JDBC_SERVER_PORT = 3387;
	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMinutes(1);
	public static final String PROPERTIES_FILE = "dataflow-jdbc-server.properties";

	@Inject
	protected Reactor reactor;

	@Inject
	protected ISqlDataflow sqlDataflow;

	/**
	 * Override this method to supply your dataflow schema.
	 */
	protected Module getDataflowSchemaModule() {
		return Module.empty();
	}

	@Provides
	NioReactor reactor(Config config, OptionalDependency<ThrottlingController> throttlingController) {
		return Eventloop.builder()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.withInspector(throttlingController.orElse(null))
				.build();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("dataflow.jdbc.server.listenAddress", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(DEFAULT_JDBC_SERVER_HOSTNAME, DEFAULT_JDBC_SERVER_PORT)))
				.with("dataflow.jdbc.server.idleTimeout", Config.ofValue(ofDuration(), DEFAULT_IDLE_TIMEOUT))
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.builder()
						.withEffectiveConfigLogger()
						.build(),
				DataflowJdbcServerModule.create(),
				getDataflowSchemaModule()
		);
	}

	@Override
	protected final void onStart() throws Exception {
		reactor.submit(() -> sqlDataflow.query("SELECT 1")
						.then(supplier -> supplier.streamTo(StreamConsumer.skip())))
				.get();

		logger.info("Connection to partitions established");
	}

	@Override
	protected final void run() throws Exception {
		awaitShutdown();
	}
}
