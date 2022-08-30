package jdbc;

import io.activej.config.Config;
import io.activej.dataflow.SqlDataflow;
import io.activej.datastream.StreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launchers.dataflow.DataflowClientLauncher;
import io.activej.launchers.dataflow.jdbc.DataflowJdbcServerModule;
import module.MultilogDataflowClientModule;

import java.io.IOException;
import java.nio.file.Files;

public final class MultilogDataflowJdbcServerLauncher extends DataflowClientLauncher {
	public static final int DEFAULT_JDBC_SERVER_PORT = 3387;

	@Inject
	Eventloop eventloop;

	@Inject
	SqlDataflow sqlDataflow;

	@Override
	protected Module getBusinessLogicModule() {
		return Modules.combine(
				MultilogDataflowClientModule.create(),
				DataflowJdbcServerModule.create()
		);
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() throws IOException {
				return Config.create()
						.with("dataflow.jdbc.server.port", String.valueOf(DEFAULT_JDBC_SERVER_PORT))
						.with("dataflow.secondaryBufferPath", Files.createTempDirectory("secondaryBufferPath").toString())
						.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
						.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
			}
		};
	}

	@Override
	protected void onStart() throws Exception {
		eventloop.submit(() -> sqlDataflow.query("SELECT 1")
				.then(supplier -> supplier.streamTo(StreamConsumer.skip())))
				.get();

		System.out.println("Connection to partitions established");
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowJdbcServerLauncher().launch(args);
	}
}
