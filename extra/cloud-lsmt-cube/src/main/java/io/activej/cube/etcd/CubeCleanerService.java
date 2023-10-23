/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube.etcd;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.service.ReactiveService;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.ref.RefLong;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.exception.CubeException;
import io.activej.etcd.EtcdEventProcessor;
import io.activej.etcd.EtcdListener;
import io.activej.etcd.EtcdUtils;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.codec.kv.EtcdKVCodecs;
import io.activej.etcd.codec.kv.EtcdKVDecoder;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.jmx.ReactiveJmxBean;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.reactor.schedule.ScheduledRunnable;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.activej.async.function.AsyncRunnables.coalesce;
import static io.activej.cube.etcd.EtcdUtils.*;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.concurrent.CompletableFuture.failedFuture;

public final class CubeCleanerService extends AbstractReactive
	implements ReactiveService, ReactiveJmxBeanWithStats {

	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerService.class);

	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	public static final Duration DEFAULT_CLEANUP_OLDER_THAN = ApplicationSettings.getDuration(CubeCleanerService.class, "cleanupOlderThan", Duration.ofHours(1));
	public static final Duration DEFAULT_CLEANUP_RETRY = ApplicationSettings.getDuration(CubeCleanerService.class, "cleanupRetry", Duration.ofMinutes(1));
	private static final Duration WATCH_RETRY_INTERVAL = ApplicationSettings.getDuration(CubeCleanerService.class, "watchRetryInterval", Duration.ofSeconds(1));

	private final Client client;
	private final AggregationChunkStorage<Long> storage;
	private final ByteSequence root;

	private final Queue<DeletedChunksEntry> deletedChunksQueue = new ConcurrentLinkedQueue<>();

	private final AsyncRunnable cleanup = coalesce(this::doCleanup);

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private EtcdKeyCodec<Long> chunkIdCodec = CHUNK_ID_CODEC;

	private ByteSequence prefixChunk = CHUNK;
	private ByteSequence timestampKey = TIMESTAMP;
	private ByteSequence cleanupRevisionKey = CLEANUP_REVISION;

	private Watch.Watcher watcher;

	private long watchRevision;
	private long lastCleanupRevision;

	private long cleanupOlderThanMillis = DEFAULT_CLEANUP_OLDER_THAN.toMillis();
	private long cleanupRetryMillis = DEFAULT_CLEANUP_RETRY.toMillis();

	private volatile boolean stopped;
	private @Nullable ScheduledRunnable cleanupSchedule;

	// region JMX
	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseDeleteChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseUpdateLastCleanupRevision = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final ExceptionStats watchEtcdExceptionStats = ExceptionStats.create();
	private final ExceptionStats malformedDataExceptionStats = ExceptionStats.create();
	private Instant watchConnectionLastEstablishedAt = null;
	private Instant watchLastCompletedAt = null;
	// endregion

	private CurrentTimeProvider now = reactor;

	private CubeCleanerService(Client client, AggregationChunkStorage<Long> storage, ByteSequence root) {
		super(storage.getReactor());
		this.client = client;
		this.storage = storage;
		this.root = root;
	}

	public static CubeCleanerService create(Client client, AggregationChunkStorage<Long> storage, ByteSequence root) {
		return builder(client, storage, root).build();
	}

	public static Builder builder(Client client, AggregationChunkStorage<Long> storage, ByteSequence root) {
		return new CubeCleanerService(client, storage, root).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeCleanerService> {
		private Builder() {}

		public Builder withCurrentTimeProvider(CurrentTimeProvider now) {
			checkNotBuilt(this);
			CubeCleanerService.this.now = now;
			return this;
		}

		public Builder withPrefixChunk(ByteSequence prefixChunk) {
			checkNotBuilt(this);
			CubeCleanerService.this.prefixChunk = prefixChunk;
			return this;
		}

		public Builder withTimestampKey(ByteSequence timestampKey) {
			checkNotBuilt(this);
			CubeCleanerService.this.timestampKey = timestampKey;
			return this;
		}

		public Builder withCleanupRevisionKey(ByteSequence cleanupRevisionKey) {
			checkNotBuilt(this);
			CubeCleanerService.this.cleanupRevisionKey = cleanupRevisionKey;
			return this;
		}

		public Builder withCleanupOlderThen(Duration cleanupOlderThan) {
			checkNotBuilt(this);
			CubeCleanerService.this.cleanupOlderThanMillis = cleanupOlderThan.toMillis();
			return this;
		}

		public Builder withCleanupRetry(Duration cleanupRetry) {
			checkNotBuilt(this);
			CubeCleanerService.this.cleanupRetryMillis = cleanupRetry.toMillis();
			return this;
		}

		public Builder withAggregationIdCodec(EtcdPrefixCodec<String> aggregationIdCodec) {
			checkNotBuilt(this);
			CubeCleanerService.this.aggregationIdCodec = aggregationIdCodec;
			return this;
		}

		public Builder withChunkIdCodec(EtcdKeyCodec<Long> chunkIdCodec) {
			checkNotBuilt(this);
			CubeCleanerService.this.chunkIdCodec = chunkIdCodec;
			return this;
		}

		@Override
		protected CubeCleanerService doBuild() {
			return CubeCleanerService.this;
		}
	}

	@Override
	public Promise<Void> start() {
		ByteSequence revisionKey = root.concat(cleanupRevisionKey);
		return Promise.ofCompletionStage(client.getKVClient().get(revisionKey)
				.exceptionallyCompose(e -> failedFuture(new CubeException("Could not get revision key", e.getCause())))
			)
			.whenResult(response -> {
				List<KeyValue> kvs = response.getKvs();
				if (kvs.isEmpty()) {
					throw new IllegalStateException("No cleanup revision is found on key '" +
													revisionKey + '\'');
				}
				assert kvs.size() == 1;
				KeyValue keyValue = kvs.get(0);

				this.lastCleanupRevision = REVISION_CODEC.decodeValue(keyValue.getValue());
				this.watcher = createWatcher();
			})
			.then(this::cleanup)
			.toVoid();
	}

	@Override
	public Promise<Void> stop() {
		stopped = true;
		if (cleanupSchedule != null) {
			cleanupSchedule.cancel();
		}
		if (this.watcher != null) {
			this.watcher.close();
		}
		return Promise.complete();
	}

	@VisibleForTesting
	Promise<Void> cleanup() {
		checkInReactorThread(reactor);
		return cleanup.run();
	}

	private Promise<Void> doCleanup() {
		checkInReactorThread(reactor);

		if (cleanupSchedule != null) cleanupSchedule.cancel();
		if (stopped) return Promise.complete();

		DeletedChunksEntry chunkEntry = deletedChunksQueue.peek();
		if (chunkEntry == null) {
			logger.trace("No chunks to be cleaned up");
			return Promise.complete();
		}

		long cleanupStartTimestamp = now.currentTimeMillis();

		if (chunkEntry.deleteTimestamp() + cleanupOlderThanMillis > cleanupStartTimestamp) {
			long nextCleanupTimestamp = chunkEntry.deleteTimestamp + cleanupOlderThanMillis + 1L;
			if (logger.isTraceEnabled()) {
				logger.trace("There are chunks to be cleaned up later, at {}", Instant.ofEpochMilli(nextCleanupTimestamp));
			}
			cleanupSchedule = reactor.scheduleBackground(nextCleanupTimestamp, this::cleanup);
			return Promise.complete();
		}

		return doCleanup(chunkEntry)
			.whenResult(() -> deletedChunksQueue.remove())
			.whenException(e -> {
				logger.warn("Failed to cleanup chunks", e);
				if (stopped) return;

				long nextCleanupAt = now.currentTimeMillis() + cleanupRetryMillis;
				if (logger.isTraceEnabled()) {
					logger.trace("Scheduling next cleanup at {}", Instant.ofEpochMilli(nextCleanupAt));
				}
				cleanupSchedule = reactor.scheduleBackground(nextCleanupAt, this::cleanup);
			})
			.then(() -> doCleanup());
	}

	private Promise<Void> doCleanup(DeletedChunksEntry entry) {
		logger.trace("Chunks to be cleaned up: {}", entry.chunkIds());

		return deleteChunksFromStorage(entry.chunkIds())
			.then(() -> updateLastCleanupRevision(lastCleanupRevision))
			.whenResult(() -> logger.trace("Chunks successfully cleaned up"))
			.whenComplete(promiseCleanup.recordStats());
	}

	private Promise<Void> deleteChunksFromStorage(Set<Long> deletedChunks) {
		return storage.deleteChunks(deletedChunks)
			.mapException(e -> new CubeException("Failed to delete chunks from storage", e))
			.whenComplete(promiseDeleteChunks.recordStats());
	}

	private Promise<Void> updateLastCleanupRevision(long lastCleanupRevision) {
		ByteSequence value = REVISION_CODEC.encodeValue(lastCleanupRevision);
		return Promise.ofCompletionStage(client.getKVClient().put(root.concat(cleanupRevisionKey), value)
				.exceptionallyCompose(e -> failedFuture(new CubeException("Failed to update last cleanup revision", e.getCause()))))
			.whenResult(() -> this.lastCleanupRevision = lastCleanupRevision)
			.whenComplete(promiseUpdateLastCleanupRevision.recordStats())
			.toVoid();
	}

	private Watch.Watcher createWatcher() {
		long revision = watchRevision == 0 ? lastCleanupRevision : (watchRevision + 1);
		return EtcdUtils.watch(client.getWatchClient(), revision,
			new EtcdUtils.WatchRequest[]{
				new EtcdUtils.WatchRequest<>(
					root.concat(timestampKey),
					EtcdKVCodecs.ofEmptyKey(EtcdUtils.TOUCH_TIMESTAMP_CODEC),
					new EtcdEventProcessor<Void, Long, RefLong>() {
						@Override
						public RefLong createEventsAccumulator() {
							return new RefLong(-1L);
						}

						@Override
						public void onPut(RefLong accumulator, Long timestamp) {
							accumulator.set(timestamp);
						}

						@Override
						public void onDelete(RefLong accumulator, Void key) {
							throw new UnsupportedOperationException();
						}
					}),
				EtcdUtils.WatchRequest.<Long, Long, Set<Long>>of(
					root.concat(prefixChunk),
					new EtcdKVDecoder<>() {
						@Override
						public Long decodeKV(io.activej.etcd.codec.kv.KeyValue kv) throws MalformedEtcdDataException {
							ByteSequence suffix = aggregationIdCodec.decodePrefix(kv.key()).suffix();
							return chunkIdCodec.decodeKey(suffix);
						}

						@Override
						public Long decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
							ByteSequence suffix = aggregationIdCodec.decodePrefix(byteSequence).suffix();
							return chunkIdCodec.decodeKey(suffix);
						}
					},
					new EtcdEventProcessor<>() {
						@Override
						public Set<Long> createEventsAccumulator() {
							return new HashSet<>();
						}

						@Override
						public void onPut(Set<Long> accumulator, Long key) {
						}

						@Override
						public void onDelete(Set<Long> accumulator, Long key) {
							accumulator.add(key);
						}
					}
				)
			},
			new EtcdListener<>() {
				@Override
				public void onConnectionEstablished() {
					logger.trace("Watch connection to etcd server established");
					watchConnectionLastEstablishedAt = now.currentInstant();
				}

				@SuppressWarnings("unchecked")
				@Override
				public void onNext(long revision, Object[] operation) {
					watchRevision = revision;
					RefLong timestampRef = (RefLong) operation[0];
					Set<Long> deletedChunks = (Set<Long>) operation[1];

					if (deletedChunks.isEmpty()) return;

					long timestamp = timestampRef.get();
					if (timestamp == -1) {
						logger.warn("No transaction timestamp found, skip deleting chunks {}", deletedChunks);
						return;
					}

					deletedChunksQueue.add(new DeletedChunksEntry(revision, timestamp, deletedChunks));
					reactor.execute(() -> {
						if (cleanupSchedule == null) {
							cleanup();
						}
					});
				}

				@Override
				public void onError(Throwable throwable) {
					logger.warn("Error occurred while watching chunks to be cleaned up", throwable);
					watchEtcdExceptionStats.recordException(throwable, this);
					if (throwable instanceof MalformedEtcdDataException) {
						malformedDataExceptionStats.recordException(throwable, this);
					}
					watcher.close();
				}

				@Override
				public void onCompleted() {
					logger.warn("Watch has been completed");
					watchLastCompletedAt = now.currentInstant();

					//noinspection DataFlowIssue
					reactor.execute(() ->
						reactor.delayBackground(WATCH_RETRY_INTERVAL, () -> {
							if (stopped) return;
							logger.trace("Recreating watcher");
							CubeCleanerService.this.watcher = createWatcher();
						}));
				}
			});
	}

	public record DeletedChunksEntry(long deleteRevision, long deleteTimestamp, Set<Long> chunkIds) {
	}

	// region JMX getters
	@JmxAttribute
	public String getRootEtcdKey() {
		return root.toString();
	}

	@JmxAttribute
	public String getCubeEtcdPrefix() {
		return prefixChunk.toString();
	}

	@JmxAttribute
	public String getCleanupRevisionEtcdKey() {
		return cleanupRevisionKey.toString();
	}

	@JmxAttribute
	public Duration getCleanupOlderThan() {
		return Duration.ofMillis(cleanupOlderThanMillis);
	}

	@JmxAttribute
	public void setCleanupOlderThan(Duration cleanupOlderThan) {
		this.cleanupOlderThanMillis = cleanupOlderThan.toMillis();
		cleanup();
	}

	@JmxAttribute
	public Duration getCleanupRetryInterval() {
		return Duration.ofMillis(cleanupRetryMillis);
	}

	@JmxAttribute
	public void setCleanupRetryInterval(Duration cleanupRetryInterval) {
		this.cleanupRetryMillis = cleanupRetryInterval.toMillis();
	}

	@JmxAttribute
	public long getLastCleanupRevision() {
		return lastCleanupRevision;
	}

	@JmxAttribute
	public long getWatchRevisionRevision() {
		return watchRevision;
	}

	@JmxAttribute
	public boolean isStopped() {
		return stopped;
	}

	@JmxAttribute
	public int getCurrentDeletedChunksQueueSize() {
		return deletedChunksQueue.size();
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanup() {
		return promiseCleanup;
	}

	@JmxAttribute
	public PromiseStats getPromiseDeleteChunks() {
		return promiseDeleteChunks;
	}

	@JmxAttribute
	public PromiseStats getPromiseUpdateLastCleanupRevision() {
		return promiseUpdateLastCleanupRevision;
	}

	@JmxAttribute
	public ExceptionStats getWatchEtcdExceptionStats() {
		return watchEtcdExceptionStats;
	}

	@JmxAttribute
	public ExceptionStats getMalformedDataExceptionStats() {
		return malformedDataExceptionStats;
	}

	@JmxAttribute
	public Instant getWatchLastCompletedAtAt() {
		return watchLastCompletedAt;
	}

	@JmxAttribute
	public Instant getWatchConnectionLastEstablishedAt() {
		return watchConnectionLastEstablishedAt;
	}

	@JmxAttribute
	public String getEtcdRoot() {
		return root.toString();
	}
	// endregion
}
