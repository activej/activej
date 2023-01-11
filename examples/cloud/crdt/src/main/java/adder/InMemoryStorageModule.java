package adder;

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.local.CrdtStorage_Map;
import io.activej.crdt.wal.AsyncWriteAheadLog;
import io.activej.crdt.wal.WriteAheadLog_InMemory;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.crdt.Local;
import io.activej.reactor.Reactor;

public final class InMemoryStorageModule extends AbstractModule {

	@Provides
	AsyncWriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Reactor reactor,
			CrdtFunction<DetailedSumsCrdtState> function,
			AsyncCrdtStorage<Long, DetailedSumsCrdtState> storage
	) {
		return WriteAheadLog_InMemory.create(reactor, function, storage);
	}

	@Provides
	@Local
	AsyncCrdtStorage<Long, DetailedSumsCrdtState> storage(Reactor reactor, CrdtFunction<DetailedSumsCrdtState> function) {
		return CrdtStorage_Map.create(reactor, function);
	}
}
