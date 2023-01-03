package adder;

import io.activej.async.service.TaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.wal.FileWriteAheadLog;
import io.activej.crdt.wal.WalUploader;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.crdt.Local;
import io.activej.reactor.Reactor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.async.service.TaskScheduler.Schedule.ofInterval;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.config.converter.ConfigConverters.ofReactorTaskSchedule;

public final class PersistentStorageModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(new Key<CrdtStorage<Long, DetailedSumsCrdtState>>(Local.class) {})
				.to(new Key<CrdtStorageFs<Long, DetailedSumsCrdtState>>() {});
	}

	@Provides
	WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Reactor reactor,
			Executor executor,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			WalUploader<Long, DetailedSumsCrdtState> uploader,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return FileWriteAheadLog.create(reactor, executor, walPath, serializer, uploader);
	}

	@Provides
	WalUploader<Long, DetailedSumsCrdtState> uploader(
			Reactor reactor,
			Executor executor,
			CrdtFunction<DetailedSumsCrdtState> function,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			CrdtStorage<Long, DetailedSumsCrdtState> storage,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return WalUploader.create(reactor, executor, walPath, function, serializer, storage);
	}

	@Provides
	CrdtStorageFs<Long, DetailedSumsCrdtState> storage(
			Reactor reactor,
			ActiveFs fs,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			CrdtFunction<DetailedSumsCrdtState> function
	) {
		return CrdtStorageFs.create(reactor, fs, serializer, function);
	}

	@Provides
	ActiveFs activeFs(Reactor reactor, Executor executor, Config config) {
		return LocalActiveFs.create(reactor, executor, config.get(ofPath(), "storage"));
	}

	@Provides
	Executor executor() {
		return Executors.newSingleThreadExecutor();
	}

	@Provides
	@Named("consolidate")
	@Eager
	TaskScheduler consolidateScheduler(Reactor reactor, CrdtStorageFs<Long, DetailedSumsCrdtState> storageFs, Config config) {
		return TaskScheduler.create(reactor, storageFs::consolidate)
				.withSchedule(config.get(ofReactorTaskSchedule(), "consolidate.schedule", ofInterval(Duration.ofMinutes(3))))
				.withInitialDelay(Duration.ofSeconds(10));
	}

}
