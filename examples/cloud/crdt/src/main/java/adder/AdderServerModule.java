package adder;

import io.activej.common.api.Initializer;
import io.activej.common.collection.CollectionUtils;
import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.hash.CrdtMap;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.service.ServiceGraphModuleSettings;

import java.util.Map;

import static adder.AdderCommands.*;

public class AdderServerModule extends AbstractModule {

	@Override
	protected void configure() {
//		install(new InMemoryStorageModule());
		install(new PersistentStorageModule());
	}

	@Provides
	@ServerId
	String serverId(Config config) {
		return config.get("serverId");
	}

	@Provides
	IdSequentialExecutor<Long> sequentialExecutor() {
		return new IdSequentialExecutor<>();
	}

	@Provides
	Map<Class<?>, RpcRequestHandler<?, ?>> handlers(
			@ServerId String serverId,
			CrdtMap<Long, SimpleSumsCrdtState> map,
			WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog,
			IdSequentialExecutor<Long> seqExecutor
	) {
		return CollectionUtils.map(
				PutRequest.class, (RpcRequestHandler<PutRequest, PutResponse>) request -> {
					long userId = request.getUserId();
					return seqExecutor.execute(userId, () -> map.get(userId)
							.then(state -> {
								float newSum = request.getDelta() +
										(state == null ?
												0 :
												state.getLocalSum());

								return writeAheadLog.put(userId, DetailedSumsCrdtState.of(serverId, newSum))
										.then(() -> map.put(userId, SimpleSumsCrdtState.of(newSum)))
										.map($ -> PutResponse.INSTANCE);
							}));
				},
				GetRequest.class, (RpcRequestHandler<GetRequest, GetResponse>) request ->
						map.get(request.getUserId())
								.map(state -> state == null ? null : state.value())
								.map(GetResponse::new)
		);
	}

	@Provides
	CrdtMap<Long, SimpleSumsCrdtState> map(Eventloop eventloop, @ServerId String serverId, CrdtStorage<Long, DetailedSumsCrdtState> storage) {
		return new AdderCrdtMap(eventloop, serverId, storage);
	}

	@Provides
	CrdtFunction<DetailedSumsCrdtState> function() {
		return new CrdtFunction<DetailedSumsCrdtState>() {
			@Override
			public DetailedSumsCrdtState merge(DetailedSumsCrdtState first, DetailedSumsCrdtState second) {
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
		return settings -> settings.addDependency(new Key<CrdtMap<Long, SimpleSumsCrdtState>>() {}, new Key<WriteAheadLog<Long, DetailedSumsCrdtState>>() {});
	}
}
