package io.activej.cube.aggregation;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.ReactiveService;
import io.activej.bytebuf.ByteBuf;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.process.frame.ChannelFrameDecoder;
import io.activej.csp.process.frame.ChannelFrameEncoder;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.cube.AggregationStructure;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.BasicStreamStats;
import io.activej.datastream.stats.DetailedStreamStats;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.minio.*;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.activej.cube.aggregation.util.Utils.createBinarySerializer;
import static io.activej.datastream.stats.StreamStatsSizeCounter.forByteBufs;
import static io.activej.reactor.Reactive.checkInReactorThread;

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public final class MinioChunkStorage extends AbstractReactive
	implements IAggregationChunkStorage, ReactiveService, ReactiveJmxBeanWithStats {

	private static final boolean CHECKS = Checks.isEnabled(AggregationChunkStorage.class);

	public static final String LOG = ".log";
	public static final int DEFAULT_PART_SIZE = ApplicationSettings.getInt(MinioChunkStorage.class, "partSize", 10485760);
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	private final AsyncSupplier<Long> idGenerator;
	private final MinioAsyncClient client;
	private final Executor executor;
	private final String bucket;

	private long partSize = DEFAULT_PART_SIZE;
	private FrameFormat frameFormat = FrameFormats.lz4();

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;
	// region JMX fields
	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(MinioChunkStorage.class, "smoothingWindow", Duration.ofMinutes(5));

	private final PromiseStats promiseAsyncSupplier = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseList = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseOpenR = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseOpenW = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseDelete = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final DetailedStreamStats<ByteBuf> readFile = StreamStats.<ByteBuf>detailedBuilder()
		.withSizeCounter(forByteBufs())
		.build();
	private final DetailedStreamStats<ByteBuf> readDecompress = StreamStats.<ByteBuf>detailedBuilder()
		.withSizeCounter(forByteBufs())
		.build();

	private final BasicStreamStats<?> readDeserialize = StreamStats.basic();
	private final DetailedStreamStats<?> readDeserializeDetailed = StreamStats.detailed();

	private final DetailedStreamStats<ByteBuf> writeFile = StreamStats.<ByteBuf>detailedBuilder()
		.withSizeCounter(forByteBufs())
		.build();
	private final DetailedStreamStats<ByteBuf> writeCompress = StreamStats.<ByteBuf>detailedBuilder()
		.withSizeCounter(forByteBufs())
		.build();

	private final BasicStreamStats<?> writeSerialize = StreamStats.basic();
	private final DetailedStreamStats<?> writeSerializeDetailed = StreamStats.detailed();

	private final ValueStats chunksCount = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats readChunks = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats writtenChunks = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats deletedChunks = EventStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean detailed;
	// endregion

	private MinioChunkStorage(
		Reactor reactor,
		AsyncSupplier<Long> idGenerator,
		MinioAsyncClient client,
		Executor executor,
		String bucket
	) {
		super(reactor);
		this.idGenerator = idGenerator;
		this.client = client;
		this.executor = executor;
		this.bucket = bucket;
	}

	public static MinioChunkStorage create(
		Reactor reactor,
		AsyncSupplier<Long> idGenerator,
		MinioAsyncClient client,
		Executor executor,
		String bucket
	) {
		return builder(reactor, idGenerator, client, executor, bucket).build();
	}

	public static MinioChunkStorage.Builder builder(
		Reactor reactor,
		AsyncSupplier<Long> idGenerator,
		MinioAsyncClient client,
		Executor executor,
		String bucket
	) {
		return new MinioChunkStorage(reactor, idGenerator, client, executor, bucket).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, MinioChunkStorage> {
		private Builder() {}

		public Builder withPartSize(int partSize) {
			checkNotBuilt(this);
			MinioChunkStorage.this.partSize = partSize;
			return this;
		}

		public Builder withFrameFormat(FrameFormat frameFormat) {
			checkNotBuilt(this);
			MinioChunkStorage.this.frameFormat = frameFormat;
			return this;
		}

		public Builder withBufferSize(MemSize bufferSize) {
			checkNotBuilt(this);
			MinioChunkStorage.this.bufferSize = bufferSize;
			return this;
		}

		@Override
		protected MinioChunkStorage doBuild() {
			return MinioChunkStorage.this;
		}
	}

	@Override
	public Promise<Long> createId() {
		if (CHECKS) checkInReactorThread(this);
		return idGenerator.get()
			.mapException(e -> new AggregationException("Could not create ID", e))
			.whenComplete(promiseAsyncSupplier.recordStats());
	}

	@Override
	public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
		if (CHECKS) checkInReactorThread(this);

		CompletableFuture<GetObjectResponse> future;

		try {
			future = client.getObject(
				GetObjectArgs.builder()
					.bucket(bucket)
					.object(toObjectName(chunkId))
					.build()
			);
		} catch (Exception e) {
			return Promise.<StreamSupplier<T>>ofException(new AggregationException("Failed to download chunk '" + chunkId + '\'', e))
				.whenComplete(promiseOpenR.recordStats());
		}

		//noinspection unchecked
		return Promise.ofCompletionStage(future)
			.mapException(e -> new AggregationException("Failed to download chunk '" + chunkId + '\'', e))
			.whenComplete(promiseOpenR.recordStats())
			.map(response -> ChannelSuppliers.ofInputStream(executor, response)
				.transformWith(readFile)
				.transformWith(ChannelFrameDecoder.create(frameFormat))
				.transformWith(readDecompress)
				.transformWith(ChannelDeserializer.create(
					createBinarySerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader)))
				.transformWith((StreamStats<T>) (detailed ? readDeserializeDetailed : readDeserialize))
				.withEndOfStream(eos -> eos
					.whenComplete(response::close)
					.whenResult(readChunks::recordEvent)
					.mapException(e -> new AggregationException("Failed to read chunk '" + chunkId + '\'', e))));
	}

	@Override
	public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
		if (CHECKS) checkInReactorThread(this);

		PipedOutputStream os = new PipedOutputStream();
		PipedInputStream is;
		try {
			is = new PipedInputStream(os);
		} catch (IOException e) {
			return Promise.<StreamConsumer<T>>ofException(new AggregationException("Failed to upload chunk '" + chunkId + '\'', e))
				.whenComplete(promiseOpenW.recordStats());
		}

		CompletableFuture<ObjectWriteResponse> future;

		try {
			future = client.putObject(
				PutObjectArgs.builder()
					.bucket(bucket)
					.object(toObjectName(chunkId))
					.stream(is, -1, partSize)
					.build());
		} catch (Exception e) {
			return Promise.<StreamConsumer<T>>ofException(new AggregationException("Failed to upload chunk '" + chunkId + '\'', e))
				.whenComplete(promiseOpenW.recordStats());
		}

		//noinspection unchecked
		return Promise.of(StreamConsumers.<T>ofSupplier(
					supplier -> supplier
						.transformWith((StreamStats<T>) (detailed ? writeSerializeDetailed : writeSerialize))
						.transformWith(ChannelSerializer.builder(
								createBinarySerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader))
							.withInitialBufferSize(bufferSize)
							.build())
						.transformWith(writeCompress)
						.transformWith(ChannelFrameEncoder.create(frameFormat))
						.transformWith(writeFile)
						.streamTo(ChannelConsumers.ofOutputStream(executor, os)))
				.withAcknowledgement(ack -> ack
					.both(Promise.ofCompletionStage(future))
					.whenResult(writtenChunks::recordEvent)
					.mapException(e -> new AggregationException("Failed to write chunk '" + chunkId + '\'', e))))
			.whenComplete(promiseOpenW.recordStats());
	}

	@Override
	public Promise<Void> finish(Set<Long> chunkIds) {
		checkInReactorThread(this);
		return Promise.complete();
	}

	@Override
	public Promise<Set<Long>> listChunks() {
		checkInReactorThread(this);

		Iterable<Result<Item>> results = client.listObjects(
			ListObjectsArgs.builder()
				.bucket(bucket)
				.build()
		);

		return Promise.ofBlocking(executor, () -> {
				Set<Long> chunks = new HashSet<>();
				for (Result<Item> result : results) {
					Item item = result.get();
					long chunkId = fromObjectName(item.objectName());
					chunks.add(chunkId);
				}
				return chunks;
			})
			.whenResult(chunks -> chunksCount.recordValue(chunks.size()))
			.whenComplete(promiseList.recordStats());
	}

	@Override
	public Promise<Void> deleteChunks(Set<Long> chunksToDelete) {
		if (CHECKS) checkInReactorThread(this);

		Iterable<Result<DeleteError>> results = client.removeObjects(
			RemoveObjectsArgs.builder()
				.bucket(bucket)
				.objects(chunksToDelete.stream()
					.map(this::toObjectName)
					.map(DeleteObject::new)
					.toList())
				.build()
		);

		return Promise.ofBlocking(executor, () -> {
				for (Result<DeleteError> result : results) {
					DeleteError deleteError;

					try {
						deleteError = result.get();
					} catch (Exception e) {
						throw new AggregationException("Failed to delete chunks", e);
					}
					if (deleteError != null) {
						throw new AggregationException("Failed to delete chunks: " + deleteError);
					}
				}
			})
			.whenComplete(promiseDelete.recordStats())
			.whenResult(() -> deletedChunks.recordEvents(chunksToDelete.size()));
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);

		CompletableFuture<Boolean> future;
		try {
			future = client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
		} catch (Exception e) {
			return Promise.ofException(new AggregationException("Failed to start storage", e));
		}

		return Promise.ofCompletionStage(future)
			.mapException(e -> new AggregationException("Failed to start storage", e))
			.whenResult(bucketExists -> {
				if (!bucketExists) {
					throw new AggregationException("Bucket " + bucket + " does not exist");
				}
			});
	}

	@Override
	public Promise<?> stop() {
		return Promise.complete();
	}

	private String toObjectName(long chunkId) {
		return ChunkIdJsonCodec.toFileName(chunkId) + LOG;
	}

	private long fromObjectName(String path) throws MalformedDataException {
		return ChunkIdJsonCodec.fromFileName(path.substring(0, path.length() - LOG.length()));
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getPromiseAsyncSupplier() {
		return promiseAsyncSupplier;
	}

	@JmxAttribute
	public PromiseStats getPromiseList() {
		return promiseList;
	}

	@JmxAttribute
	public PromiseStats getPromiseOpenR() {
		return promiseOpenR;
	}

	@JmxAttribute
	public PromiseStats getPromiseOpenW() {
		return promiseOpenW;
	}

	@JmxAttribute
	public DetailedStreamStats getReadFile() {
		return readFile;
	}

	@JmxAttribute
	public DetailedStreamStats getReadDecompress() {
		return readDecompress;
	}

	@JmxAttribute
	public BasicStreamStats getReadDeserialize() {
		return readDeserialize;
	}

	@JmxAttribute
	public DetailedStreamStats getReadDeserializeDetailed() {
		return readDeserializeDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getWriteSerialize() {
		return writeSerialize;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteSerializeDetailed() {
		return writeSerializeDetailed;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteCompress() {
		return writeCompress;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteFile() {
		return writeFile;
	}

	@JmxAttribute
	public EventStats getReadChunks() {
		return readChunks;
	}

	@JmxAttribute
	public EventStats getWrittenChunks() {
		return writtenChunks;
	}

	@JmxAttribute
	public EventStats getDeletedChunks() {
		return deletedChunks;
	}

	@JmxAttribute
	public ValueStats getChunksCount() {
		return chunksCount;
	}

	@JmxAttribute
	public PromiseStats getPromiseDelete() {
		return promiseDelete;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailed = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailed = false;
	}
	// endregion
}
