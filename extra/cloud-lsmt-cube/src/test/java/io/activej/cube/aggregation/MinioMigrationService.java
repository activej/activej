package io.activej.cube.aggregation;

import io.activej.common.exception.MalformedDataException;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.cube.CubeState;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.fs.FileMetadata;
import io.activej.fs.IFileSystem;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.minio.MinioAsyncClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Utils.difference;
import static io.activej.cube.aggregation.AggregationChunkStorage.LOG;

public final class MinioMigrationService<C> {
	private static final Logger logger = LoggerFactory.getLogger(MinioMigrationService.class);

	private final Reactor reactor;
	private final Executor executor;
	private final StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;
	private final ChunkIdJsonCodec<C> codec;
	private final IFileSystem fromFileSystem;
	private final MinioAsyncClient toClient;
	private final String bucket;

	private Set<C> chunksToMigrate;

	private MinioMigrationService(
		Reactor reactor,
		Executor executor,
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager,
		ChunkIdJsonCodec<C> codec,
		IFileSystem fromFileSystem,
		MinioAsyncClient toClient,
		String bucket
	) {
		this.reactor = reactor;
		this.executor = executor;
		this.stateManager = stateManager;
		this.codec = codec;
		this.fromFileSystem = fromFileSystem;
		this.toClient = toClient;
		this.bucket = bucket;
	}

	public static <C> CompletableFuture<Void> migrate(
		Reactor reactor,
		Executor executor,
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager,
		ChunkIdJsonCodec<C> codec,
		IFileSystem fromFileSystem,
		MinioAsyncClient toClient,
		String bucket
	) {
		return new MinioMigrationService<>(reactor, executor, stateManager, codec, fromFileSystem, toClient, bucket).migrate();
	}

	private CompletableFuture<Void> migrate() {
		return reactor.submit(() -> stateManager.catchUp()
			.whenResult(() -> {
				//noinspection unchecked
				chunksToMigrate = (Set<C>) stateManager.query(logState -> logState.getDataState().getAllChunks());

				logger.info("Migrating {} chunks", chunksToMigrate.size());
				logger.trace("Chunks to be migrated: {}", chunksToMigrate);
			})
			.then(this::collectFilesForMigration)
			.then(files -> Promises.all(files.entrySet().stream().map(this::migrateFile)))
			.whenComplete(toLogger(logger, "migrate")));
	}

	private Promise<Map<String, FileMetadata>> collectFilesForMigration() {
		return fromFileSystem.list("*" + LOG)
			.map(metadataMap -> {
				Map<String, FileMetadata> filesToMigrate = new HashMap<>(chunksToMigrate.size());
				for (C c : chunksToMigrate) {
					String fileName = codec.toFileName(c) + LOG;
					FileMetadata fileMetadata = metadataMap.get(fileName);
					if (fileMetadata != null) {
						filesToMigrate.put(fileName, fileMetadata);
						continue;
					}

					throwMissingChunks(codec, metadataMap.keySet(), chunksToMigrate);
				}
				return filesToMigrate;
			});
	}

	private Promise<Void> migrateFile(Map.Entry<String, FileMetadata> entry) {
		return fromFileSystem.download(entry.getKey())
			.then(supplier -> {
				PipedOutputStream os = new PipedOutputStream();
				PipedInputStream is = new PipedInputStream(os);

				CompletableFuture<ObjectWriteResponse> future = toClient.putObject(
					PutObjectArgs.builder()
						.bucket(bucket)
						.object(entry.getKey())
						.stream(is, entry.getValue().getSize(), -1)
						.build()
				);

				return supplier.streamTo(ChannelConsumers.ofOutputStream(executor, os))
					.both(Promise.ofCompletionStage(future));
			});
	}

	private void throwMissingChunks(ChunkIdJsonCodec<C> codec, Set<String> files, Set<C> chunksToMigrate) {
		Set<C> presentChunks = new HashSet<>(files.size());
		for (String name : files) {
			try {
				presentChunks.add(codec.fromFileName(name));
			} catch (MalformedDataException ignored) {
			}
		}
		throw new RuntimeException("Missing chunks: " + difference(chunksToMigrate, presentChunks));
	}
}
