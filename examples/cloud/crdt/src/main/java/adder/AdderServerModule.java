package adder;

import io.activej.async.service.TaskScheduler;
import io.activej.common.initializer.Initializer;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.hash.AsyncCrdtMap;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.cluster.PartitionId;
import io.activej.crdt.wal.AsyncWriteAheadLog;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.Reactor;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.service.ServiceGraphModuleSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static adder.AdderCommands.*;

public final class AdderServerModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(AdderServerModule.class);

	@Override
	protected void configure() {
		install(new ClusterStorageModule());

		install(new InMemoryStorageModule());
//		install(new PersistentStorageModule());
	}

	@Provides
	IdSequentialExecutor<Long> sequentialExecutor() {
		return new IdSequentialExecutor<>();
	}

	@Provides
	Map<Class<?>, RpcRequestHandler<?, ?>> handlers(
			PartitionId partitionId,
			AsyncCrdtMap<Long, SimpleSumsCrdtState> map,
			AsyncWriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog,
			IdSequentialExecutor<Long> seqExecutor
	) {
		return Map.of(
				AddRequest.class, (RpcRequestHandler<AddRequest, AddResponse>) request -> {
					long userId = request.userId();
					logger.info("Received 'Add' request for user {}", userId);

					return seqExecutor.execute(userId, () -> map.get(userId)
							.then(state -> {
								float newSum = request.delta() +
										(state == null ?
												0 :
												state.localSum());

								return writeAheadLog.put(userId, DetailedSumsCrdtState.of(partitionId.toString(), newSum))
										.then(() -> map.put(userId, SimpleSumsCrdtState.of(newSum)))
										.map($ -> AddResponse.INSTANCE);
							}));
				},
				GetRequest.class, (RpcRequestHandler<GetRequest, GetResponse>) request -> {
					long userId = request.userId();
					logger.info("Received 'Get' request for user {}", userId);

					return map.get(userId)
							.mapIfNonNull(SimpleSumsCrdtState::value)
							.mapIfNull(() -> 0f)
							.map(GetResponse::new);
				}
		);
	}

	@Provides
	AsyncCrdtMap<Long, SimpleSumsCrdtState> map(Reactor reactor, PartitionId partitionId, AsyncCrdtStorage<Long, DetailedSumsCrdtState> storage) {
		return new CrdtMap_Adder(reactor, partitionId.toString(), storage);
	}

	@Provides
	CrdtFunction<DetailedSumsCrdtState> function() {
		return new CrdtFunction<>() {
			@Override
			public DetailedSumsCrdtState merge(DetailedSumsCrdtState first, long firstTimestamp, DetailedSumsCrdtState second, long secondTimestamp) {
				return first.merge(second);
			}

			@Override
			public DetailedSumsCrdtState extract(DetailedSumsCrdtState state, long timestamp) {
				return state;
			}
		};
	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts CrdtMap only after it has started the WriteAheadLog
		return settings -> settings.addDependency(new Key<AsyncCrdtMap<Long, SimpleSumsCrdtState>>() {}, new Key<AsyncWriteAheadLog<Long, DetailedSumsCrdtState>>() {});
	}

	@Provides
	@Eager
	@Named("Map refresh")
	TaskScheduler mapRefresh(Reactor reactor, AsyncCrdtMap<Long, SimpleSumsCrdtState> map) {
		return TaskScheduler.create(reactor, map::refresh)
				.withInterval(Duration.ofSeconds(10));
	}
}
