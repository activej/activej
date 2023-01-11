package banner;

import io.activej.common.initializer.Initializer;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.hash.AsyncCrdtMap;
import io.activej.crdt.hash.CrdtMap_Java;
import io.activej.crdt.primitives.GSet;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.local.CrdtStorage_Map;
import io.activej.crdt.wal.AsyncWriteAheadLog;
import io.activej.crdt.wal.WriteAheadLog_InMemory;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.Reactor;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.service.ServiceGraphModuleSettings;

import java.util.Map;

import static banner.BannerCommands.*;

public class BannerServerModule extends AbstractModule {

	@Provides
	Map<Class<?>, RpcRequestHandler<?, ?>> handlers(
			AsyncCrdtMap<Long, GSet<Integer>> map,
			AsyncWriteAheadLog<Long, GSet<Integer>> writeAheadLog
	) {
		return Map.of(
				PutRequest.class, (RpcRequestHandler<PutRequest, PutResponse>) request -> {
					long userId = request.userId();
					GSet<Integer> bannerIds = GSet.of(request.bannerIds());
					return writeAheadLog.put(userId, bannerIds)
							.then(() -> map.put(userId, bannerIds))
							.map($ -> PutResponse.INSTANCE);
				},
				GetRequest.class, (RpcRequestHandler<GetRequest, GetResponse>) request1 ->
						map.get(request1.userId())
								.map(GetResponse::new), IsBannerSeenRequest.class, (RpcRequestHandler<IsBannerSeenRequest, Boolean>) request2 ->
						map.get(request2.userId())
								.map(bannerIds1 -> bannerIds1 != null && bannerIds1.contains(request2.bannerId())));
	}

	@Provides
	AsyncCrdtMap<Long, GSet<Integer>> map(Reactor reactor, AsyncCrdtStorage<Long, GSet<Integer>> storage) {
		return new CrdtMap_Java<>(reactor, GSet::merge, storage);
	}

	@Provides
	CrdtFunction<GSet<Integer>> function() {
		return new CrdtFunction<>() {
			@Override
			public GSet<Integer> merge(GSet<Integer> first, long firstTimestamp, GSet<Integer> second, long secondTimestamp) {
				return first.merge(second);
			}

			@Override
			public GSet<Integer> extract(GSet<Integer> state, long timestamp) {
				return state;
			}
		};
	}

	@Provides
	AsyncWriteAheadLog<Long, GSet<Integer>> writeAheadLog(Reactor reactor, CrdtFunction<GSet<Integer>> function, AsyncCrdtStorage<Long, GSet<Integer>> storage) {
		return WriteAheadLog_InMemory.create(reactor, function, storage);
	}

	@Provides
	AsyncCrdtStorage<Long, GSet<Integer>> storage(Reactor reactor, CrdtFunction<GSet<Integer>> function) {
		return CrdtStorage_Map.create(reactor, function);
	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts CrdtMap only after it has started the WriteAheadLog
		return settings -> settings.addDependency(new Key<AsyncCrdtMap<Long, GSet<Integer>>>() {}, new Key<AsyncWriteAheadLog<Long, GSet<Integer>>>() {});
	}
}
