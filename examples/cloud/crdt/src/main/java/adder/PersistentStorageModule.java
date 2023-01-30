package adder;

import io.activej.async.service.TaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.local.FileSystemCrdtStorage;
import io.activej.crdt.util.CrdtDataBinarySerializer;
import io.activej.crdt.wal.IWriteAheadLog;
import io.activej.crdt.wal.WalUploader;
import io.activej.crdt.wal.FileWriteAheadLog;
import io.activej.fs.IFileSystem;
import io.activej.fs.FileSystem;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.Reactor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.async.service.TaskScheduler.Schedule.ofInterval;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.config.converter.ConfigConverters.ofReactorTaskSchedule;

public final class PersistentStorageModule extends AbstractModule {

	@Provides
	IWriteAheadLog<Long, DetailedSumsCrdtState> writeAheadLog(
			Reactor reactor,
			Executor executor,
			CrdtDataBinarySerializer<Long, DetailedSumsCrdtState> serializer,
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
			CrdtDataBinarySerializer<Long, DetailedSumsCrdtState> serializer,
			ICrdtStorage<Long, DetailedSumsCrdtState> storage,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return WalUploader.create(reactor, executor, walPath, function, serializer, storage);
	}

	@Provides
	FileSystemCrdtStorage<Long, DetailedSumsCrdtState> storage(
			Reactor reactor,
			IFileSystem fs,
			CrdtDataBinarySerializer<Long, DetailedSumsCrdtState> serializer,
			CrdtFunction<DetailedSumsCrdtState> function
	) {
		return FileSystemCrdtStorage.create(reactor, fs, serializer, function);
	}

	@Provides
	IFileSystem fileSystem(Reactor reactor, Executor executor, Config config) {
		return FileSystem.create(reactor, executor, config.get(ofPath(), "storage"));
	}

	@Provides
	Executor executor() {
		return Executors.newSingleThreadExecutor();
	}

	@Provides
	@Named("consolidate")
	@Eager
	TaskScheduler consolidateScheduler(Reactor reactor, FileSystemCrdtStorage<Long, DetailedSumsCrdtState> storageFileSystem, Config config) {
		return TaskScheduler.builder(reactor, storageFileSystem::consolidate)
				.withSchedule(config.get(ofReactorTaskSchedule(), "consolidate.schedule", ofInterval(Duration.ofMinutes(3))))
				.withInitialDelay(Duration.ofSeconds(10))
				.build();
	}

}
