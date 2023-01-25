package adder;

import io.activej.async.service.TaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.local.CrdtStorage_FileSystem;
import io.activej.crdt.util.BinarySerializer_CrdtData;
import io.activej.crdt.wal.IWriteAheadLog;
import io.activej.crdt.wal.WalUploader;
import io.activej.crdt.wal.WriteAheadLog_File;
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
			BinarySerializer_CrdtData<Long, DetailedSumsCrdtState> serializer,
			WalUploader<Long, DetailedSumsCrdtState> uploader,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return WriteAheadLog_File.create(reactor, executor, walPath, serializer, uploader);
	}

	@Provides
	WalUploader<Long, DetailedSumsCrdtState> uploader(
			Reactor reactor,
			Executor executor,
			CrdtFunction<DetailedSumsCrdtState> function,
			BinarySerializer_CrdtData<Long, DetailedSumsCrdtState> serializer,
			ICrdtStorage<Long, DetailedSumsCrdtState> storage,
			Config config
	) {
		Path walPath = config.get(ofPath(), "wal-storage");
		return WalUploader.create(reactor, executor, walPath, function, serializer, storage);
	}

	@Provides
	CrdtStorage_FileSystem<Long, DetailedSumsCrdtState> storage(
			Reactor reactor,
			IFileSystem fs,
			BinarySerializer_CrdtData<Long, DetailedSumsCrdtState> serializer,
			CrdtFunction<DetailedSumsCrdtState> function
	) {
		return CrdtStorage_FileSystem.create(reactor, fs, serializer, function);
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
	TaskScheduler consolidateScheduler(Reactor reactor, CrdtStorage_FileSystem<Long, DetailedSumsCrdtState> storageFileSystem, Config config) {
		return TaskScheduler.builder(reactor, storageFileSystem::consolidate)
				.withSchedule(config.get(ofReactorTaskSchedule(), "consolidate.schedule", ofInterval(Duration.ofMinutes(3))))
				.withInitialDelay(Duration.ofSeconds(10))
				.build();
	}

}
