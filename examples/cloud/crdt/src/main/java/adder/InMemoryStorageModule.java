package adder;

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.local.MapCrdtStorage;
import io.activej.crdt.wal.IWriteAheadLog;
import io.activej.crdt.wal.InMemoryWriteAheadLog;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.crdt.Local;
import io.activej.reactor.Reactor;

public final class InMemoryStorageModule extends AbstractModule {

	@Provides
	IWriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Reactor reactor,
			CrdtFunction<DetailedSumsCrdtState> function,
			ICrdtStorage<Long, DetailedSumsCrdtState> storage
	) {
		return InMemoryWriteAheadLog.create(reactor, function, storage);
	}

	@Provides
	@Local
	ICrdtStorage<Long, DetailedSumsCrdtState> storage(Reactor reactor, CrdtFunction<DetailedSumsCrdtState> function) {
		return MapCrdtStorage.create(reactor, function);
	}
}
