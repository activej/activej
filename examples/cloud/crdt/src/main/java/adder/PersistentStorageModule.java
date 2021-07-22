package adder;

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.wal.FileWriteAheadLog;
import io.activej.crdt.wal.WalUploader;
import io.activej.crdt.wal.WriteAheadLog;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.async.service.EventloopTaskScheduler.Schedule.ofInterval;
import static io.activej.config.converter.ConfigConverters.ofEventloopTaskSchedule;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;

public final class PersistentStorageModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(new Key<CrdtStorage<Long, DetailedSumsCrdtState>>() {})
				.to(new Key<CrdtStorageFs<Long, DetailedSumsCrdtState>>() {});
	}

	@Provides
	WriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Eventloop eventloop,
			Executor executor,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			WalUploader<Long, DetailedSumsCrdtState> uploader,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return FileWriteAheadLog.create(eventloop, executor, walPath, serializer, uploader);
	}

	@Provides
	WalUploader<Long, DetailedSumsCrdtState> uploader(
			Eventloop eventloop,
			Executor executor,
			CrdtFunction<DetailedSumsCrdtState> function,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			CrdtStorage<Long, DetailedSumsCrdtState> storage,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return WalUploader.create(eventloop, executor, walPath, function, serializer, storage);
	}

	@Provides
	CrdtStorageFs<Long, DetailedSumsCrdtState> storage(
			Eventloop eventloop,
			ActiveFs fs,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			CrdtFunction<DetailedSumsCrdtState> function
	) {
		return CrdtStorageFs.create(eventloop, fs, serializer, function);
	}

	@Provides
	ActiveFs activeFs(Eventloop eventloop, Executor executor, Config config) {
		return LocalActiveFs.create(eventloop, executor, config.get(ofPath(), "storage"));
	}

	@Provides
	Executor executor() {
		return Executors.newSingleThreadExecutor();
	}

	@Provides
	CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer() {
		return new CrdtDataSerializer<>(LONG_SERIALIZER, DetailedSumsCrdtState.SERIALIZER);
	}

	@Provides
	@Named("consolidate")
	@Eager
	EventloopTaskScheduler consolidateScheduler(Eventloop eventloop, CrdtStorageFs<Long, DetailedSumsCrdtState> storageFs, Config config) {
		return EventloopTaskScheduler.create(eventloop, storageFs::consolidate)
				.withSchedule(config.get(ofEventloopTaskSchedule(), "consolidate.schedule", ofInterval(Duration.ofMinutes(3))))
				.withInitialDelay(Duration.ofMinutes(3));
	}

}
