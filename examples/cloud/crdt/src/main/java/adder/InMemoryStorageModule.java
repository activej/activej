package adder;

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.wal.InMemoryWriteAheadLog;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

public final class InMemoryStorageModule extends AbstractModule {

	@Provides
	WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Eventloop eventloop,
			CrdtFunction<DetailedSumsCrdtState> function,
			CrdtStorage<Long, DetailedSumsCrdtState> storage
	) {
		return InMemoryWriteAheadLog.create(eventloop, function, storage);
	}

	@Provides
	CrdtStorage<Long, DetailedSumsCrdtState> storage(Eventloop eventloop, CrdtFunction<DetailedSumsCrdtState> function) {
		return CrdtStorageMap.create(eventloop, function);
	}
}
