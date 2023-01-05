package worker;

import io.activej.common.ApplicationSettings;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launchers.dataflow.DataflowServerLauncher;
import io.activej.multilog.AsyncMultilog;
import io.activej.reactor.Reactor;
import misc.LogItem;
import module.MultilogDataflowServerModule;

public final class MultilogDataflowWorkerServerLauncher extends DataflowServerLauncher {
	public static final int NUMBER_OF_ITEMS = ApplicationSettings.getInt(MultilogDataflowWorkerServerLauncher.class, "numberOfItems", 100);

	@Inject
	Reactor reactor;

	@Inject
	AsyncMultilog<LogItem> multilog;

	@Inject
	@Named("partition")
	String partitionId;

	@Provides
	Reactor eventloop() {
		return Eventloop.create();
	}

	@Override
	protected Module getBusinessLogicModule() {
		return MultilogDataflowServerModule.create();
	}

	@Override
	protected void run() throws Exception {
		reactor.submit(() ->
						StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(NUMBER_OF_ITEMS))
								.streamTo(multilog.write(partitionId)))
				.get();

		super.run();
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowWorkerServerLauncher().launch(args);
	}
}
