package worker;

import io.activej.common.ApplicationSettings;
import io.activej.config.Config;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.dataflow.DataflowServerLauncher;
import io.activej.multilog.Multilog;
import misc.LogItem;
import module.MultilogDataflowServerModule;

import java.io.IOException;
import java.nio.file.Files;

public final class MultilogDataflowWorkerServerLauncher extends DataflowServerLauncher {
	public static final int NUMBER_OF_ITEMS = ApplicationSettings.getInt(MultilogDataflowWorkerServerLauncher.class, "numberOfItems", 100);

	@Inject
	Eventloop eventloop;

	@Inject
	Multilog<LogItem> multilog;

	@Inject
	@Named("partition")
	String partitionId;

	@Override
	protected Module getBusinessLogicModule() {
		return MultilogDataflowServerModule.create();
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() throws IOException {
				return Config.create()
						.with("dataflow.secondaryBufferPath", Files.createTempDirectory("secondaryBufferPath").toString())
						.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
						.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
			}
		};
	}

	@Override
	protected void run() throws Exception {
		eventloop.submit(() ->
						StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(NUMBER_OF_ITEMS))
								.streamTo(multilog.write(partitionId)))
				.get();

		super.run();
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowWorkerServerLauncher().launch(args);
	}
}
