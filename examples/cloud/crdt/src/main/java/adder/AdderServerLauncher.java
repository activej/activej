package adder;

import adder.AdderCommands.AddRequest;
import adder.AdderCommands.AddResponse;
import adder.AdderCommands.GetRequest;
import adder.AdderCommands.GetResponse;
import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.ConfigModule;
import io.activej.crdt.hash.CrdtMap;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.launchers.crdt.Local;
import io.activej.launchers.crdt.rpc.CrdtRpcServerModule;
import io.activej.service.ServiceGraphModule;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public final class AdderServerLauncher extends Launcher {
	public static final List<Class<?>> MESSAGE_TYPES = unmodifiableList(asList(
			GetRequest.class, GetResponse.class,
			AddRequest.class, AddResponse.class
	));

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				new CrdtRpcServerModule<Long, DetailedSumsCrdtState>() {
					@Override
					protected List<Class<?>> getMessageTypes() {
						return MESSAGE_TYPES;
					}
				},
				new AdderServerModule()
		);
	}

	@Eager
	@Provides
	@Named("Print local data")
	EventloopTaskScheduler printLocalMap(Eventloop eventloop, CrdtMap<Long, SimpleSumsCrdtState> map, @Local CrdtStorage<Long, DetailedSumsCrdtState> storage) {
		checkState((map instanceof AdderCrdtMap));

		return EventloopTaskScheduler.create(eventloop, () -> storage.download()
						.then(StreamSupplier::toList)
						.whenResult(crdtData -> {
							logger.info("\nLocal storage data: \n{}" +
											"\nLocal map data: \n{}",
									crdtData.stream()
											.map(data -> data.getKey() + ": " + data.getState().getSum())
											.collect(Collectors.joining("\n")),
									((AdderCrdtMap) map).getMap().entrySet().stream()
											.map(data -> data.getKey() + ": " + data.getValue().value())
											.collect(Collectors.joining("\n")));
						}))
				.withInterval(Duration.ofSeconds(10));
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new AdderServerLauncher().launch(args);
	}
}
