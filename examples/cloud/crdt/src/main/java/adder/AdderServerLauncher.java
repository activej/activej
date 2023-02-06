package adder;

import adder.AdderCommands.AddRequest;
import adder.AdderCommands.AddResponse;
import adder.AdderCommands.GetRequest;
import adder.AdderCommands.GetResponse;
import io.activej.async.service.TaskScheduler;
import io.activej.config.ConfigModule;
import io.activej.crdt.hash.ICrdtMap;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.launchers.crdt.Local;
import io.activej.launchers.crdt.rpc.CrdtRpcServerModule;
import io.activej.reactor.Reactor;
import io.activej.service.ServiceGraphModule;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkState;

public final class AdderServerLauncher extends Launcher {
	public static final List<Class<?>> MESSAGE_TYPES = List.of(GetRequest.class, GetResponse.class, AddRequest.class, AddResponse.class);

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.builder()
						.withEffectiveConfigLogger()
						.build(),
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
	TaskScheduler printLocalMap(Reactor reactor, ICrdtMap<Long, SimpleSumsCrdtState> map, @Local ICrdtStorage<Long, DetailedSumsCrdtState> storage) {
		checkState((map instanceof AdderCrdtMap));

		return TaskScheduler.builder(reactor, () -> storage.download()
						.then(StreamSupplier::toList)
						.whenResult(crdtData -> logger.info("""

										Local storage data:\s
										{}
										Local map data:\s
										{}""",
								crdtData.stream()
										.map(data -> data.getKey() + ": " + data.getState().getSum())
										.collect(Collectors.joining("\n")),
								((AdderCrdtMap) map).getMap().entrySet().stream()
										.map(data -> data.getKey() + ": " + data.getValue().value())
										.collect(Collectors.joining("\n")))))
				.withInterval(Duration.ofSeconds(10))
				.build();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new AdderServerLauncher().launch(args);
	}
}
