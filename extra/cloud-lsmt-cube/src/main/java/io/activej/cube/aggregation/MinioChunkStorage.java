package io.activej.cube.aggregation;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.ReactiveService;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
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
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.activej.cube.aggregation.util.Utils.createBinarySerializer;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class MinioChunkStorage<C> extends AbstractReactive
	implements IAggregationChunkStorage<C>, ReactiveService, ReactiveJmxBeanWithStats {

	private static final boolean CHECKS = Checks.isEnabled(AggregationChunkStorage.class);

	public static final String LOG = ".log";
	public static final int DEFAULT_PART_SIZE = ApplicationSettings.getInt(MinioChunkStorage.class, "partSize", 10485760);

	private final ChunkIdJsonCodec<C> chunkIdCodec;
	private final AsyncSupplier<C> idGenerator;
	private final MinioAsyncClient client;
	private final Executor executor;
	private final String bucket;

	private long partSize = DEFAULT_PART_SIZE;
	private FrameFormat frameFormat = FrameFormats.lz4();

	private MinioChunkStorage(
		Reactor reactor,
		ChunkIdJsonCodec<C> chunkIdCodec,
		AsyncSupplier<C> idGenerator,
		MinioAsyncClient client,
		Executor executor,
		String bucket
	) {
		super(reactor);
		this.chunkIdCodec = chunkIdCodec;
		this.idGenerator = idGenerator;
		this.client = client;
		this.executor = executor;
		this.bucket = bucket;
	}

	public static <C> MinioChunkStorage<C> create(
		Reactor reactor,
		ChunkIdJsonCodec<C> chunkIdCodec,
		AsyncSupplier<C> idGenerator,
		MinioAsyncClient client,
		Executor executor,
		String bucket
	) {
		return builder(reactor, chunkIdCodec, idGenerator, client, executor, bucket).build();
	}

	public static <C> MinioChunkStorage<C>.Builder builder(
		Reactor reactor,
		ChunkIdJsonCodec<C> chunkIdCodec,
		AsyncSupplier<C> idGenerator,
		MinioAsyncClient client,
		Executor executor,
		String bucket
	) {
		return new MinioChunkStorage<>(reactor, chunkIdCodec, idGenerator, client, executor, bucket).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, MinioChunkStorage<C>> {
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

		@Override
		protected MinioChunkStorage<C> doBuild() {
			return MinioChunkStorage.this;
		}
	}

	@Override
	public Promise<C> createId() {
		if (CHECKS) checkInReactorThread(this);
		return idGenerator.get()
			.mapException(e -> new AggregationException("Could not create ID", e));
	}

	@Override
	public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, C chunkId, DefiningClassLoader classLoader) {
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
			return Promise.ofException(new AggregationException("Failed to download chunk '" + chunkId + '\'', e));
		}

		return Promise.ofCompletionStage(future)
			.mapException(e -> new AggregationException("Failed to download chunk '" + chunkId + '\'', e))
			.map(response -> ChannelSuppliers.ofInputStream(executor, response)
				.transformWith(ChannelFrameDecoder.create(frameFormat))
				.transformWith(ChannelDeserializer.create(
					createBinarySerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader)))
				.withEndOfStream(eos -> eos
					.whenComplete(response::close)
					.mapException(e -> new AggregationException("Failed to read chunk '" + chunkId + '\'', e))));
	}

	@Override
	public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, C chunkId, DefiningClassLoader classLoader) {
		if (CHECKS) checkInReactorThread(this);

		PipedOutputStream os = new PipedOutputStream();
		PipedInputStream is;
		try {
			is = new PipedInputStream(os);
		} catch (IOException e) {
			return Promise.ofException(new AggregationException("Failed to upload chunk '" + chunkId + '\'', e));
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
			return Promise.ofException(new AggregationException("Failed to upload chunk '" + chunkId + '\'', e));
		}

		return Promise.of(StreamConsumers.<T>ofSupplier(
				supplier -> supplier
					.transformWith(ChannelSerializer.builder(
							createBinarySerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader))
						.build())
					.transformWith(ChannelFrameEncoder.create(frameFormat))
					.streamTo(ChannelConsumers.ofOutputStream(executor, os)))
			.withAcknowledgement(ack -> ack
				.both(Promise.ofCompletionStage(future))
				.mapException(e -> new AggregationException("Failed to write chunk '" + chunkId + '\'', e))));
	}

	@Override
	public Promise<Void> finish(Set<C> chunkIds) {
		checkInReactorThread(this);
		return Promise.complete();
	}

	@Override
	public Promise<Set<C>> listChunks() {
		checkInReactorThread(this);

		Iterable<Result<Item>> results = client.listObjects(
			ListObjectsArgs.builder()
				.bucket(bucket)
				.build()
		);

		return Promise.ofBlocking(executor, () -> {
			Set<C> chunks = new HashSet<>();
			for (Result<Item> result : results) {
				Item item = result.get();
				C chunkId = fromObjectName(item.objectName());
				chunks.add(chunkId);
			}
			return chunks;
		});
	}

	@Override
	public Promise<Void> deleteChunks(Set<C> chunksToDelete) {
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
		});
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

	private String toObjectName(C chunkId) {
		return chunkIdCodec.toFileName(chunkId) + LOG;
	}

	private C fromObjectName(String path) throws MalformedDataException {
		return chunkIdCodec.fromFileName(path.substring(0, path.length() - LOG.length()));
	}
}
