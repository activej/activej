package adder;

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.wal.InMemoryWriteAheadLog;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.crdt.Local;
import io.activej.reactor.Reactor;

public final class InMemoryStorageModule extends AbstractModule {

	@Provides
	WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Reactor reactor,
			CrdtFunction<DetailedSumsCrdtState> function,
			CrdtStorage<Long, DetailedSumsCrdtState> storage
	) {
		return InMemoryWriteAheadLog.create(reactor, function, storage);
	}

	@Provides
	@Local
	CrdtStorage<Long, DetailedSumsCrdtState> storage(Reactor reactor, CrdtFunction<DetailedSumsCrdtState> function) {
		return CrdtStorageMap.create(reactor, function);
	}
}
