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
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.exception.CubeException;
import io.activej.etcd.EtcdUtils;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.codec.prefix.EtcdPrefixCodecs;
import io.activej.etcd.codec.prefix.Prefix;
import io.activej.etcd.codec.value.EtcdValueCodec;
import io.activej.etcd.codec.value.EtcdValueCodecs;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.jmx.ReactiveJmxBean;
import io.activej.reactor.schedule.ScheduledRunnable;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent.EventType;
import io.etcd.jetcd.watch.WatchResponse;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.activej.async.function.AsyncRunnables.coalesce;
import static io.activej.cube.etcd.EtcdUtils.CHUNK_ID_CODEC;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CubeCleanerService extends AbstractReactive
	implements ReactiveService, ReactiveJmxBean {

	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerService.class);

	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private static final ByteSequence CUBE = byteSequenceFrom("cube.");
	private static final EtcdPrefixCodec<String> AGGREGATION_ID_CODEC = EtcdPrefixCodecs.ofTerminatingString('.');
	private static final EtcdValueCodec<Long> REVISION_CODEC = EtcdValueCodecs.ofLongString();

	public static final Duration DEFAULT_CLEANUP_OLDER_THAN = ApplicationSettings.getDuration(CubeCleanerService.class, "cleanupOlderThan", Duration.ofHours(1));
	public static final Duration DEFAULT_CLEANUP_RETRY = ApplicationSettings.getDuration(CubeCleanerService.class, "cleanupRetry", Duration.ofMinutes(1));

	private final Client client;
	private final AggregationChunkStorage<Long> storage;
	private final ByteSequence root;
	private final ByteSequence cleanupRevisionKey;

	private final Queue<DeletedChunksEntry> deletedChunksQueue = new ConcurrentLinkedQueue<>();

	private final AsyncRunnable cleanup = coalesce(this::doCleanup);

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private EtcdKeyCodec<Long> chunkIdCodec = CHUNK_ID_CODEC;

	private ByteSequence prefixCube = CUBE;

	private @Nullable Watch.Watcher watcher;

	private long lastCleanupRevision;

	private long cleanupOlderThanMillis = DEFAULT_CLEANUP_OLDER_THAN.toMillis();
	private long cleanupRetryMillis = DEFAULT_CLEANUP_RETRY.toMillis();

	private volatile boolean stopped;
	private @Nullable ScheduledRunnable cleanupSchedule;

	// region JMX
	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseDeleteChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseUpdateLastCleanupRevision = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private CurrentTimeProvider now = reactor;

	private CubeCleanerService(Client client, AggregationChunkStorage<Long> storage, ByteSequence root, ByteSequence cleanupRevisionKey) {
		super(storage.getReactor());
		this.client = client;
		this.storage = storage;
		this.root = root;
		this.cleanupRevisionKey = cleanupRevisionKey;
	}

	public static CubeCleanerService create(Client client, AggregationChunkStorage<Long> storage, ByteSequence root, ByteSequence cleanupRevisionKey) {
		return builder(client, storage, root, cleanupRevisionKey).build();
	}

	public static Builder builder(Client client, AggregationChunkStorage<Long> storage, ByteSequence root, ByteSequence cleanupRevisionKey) {
		return new CubeCleanerService(client, storage, root, cleanupRevisionKey).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeCleanerService> {
		private Builder() {}

		public Builder withCurrentTimeProvider(CurrentTimeProvider now) {
			checkNotBuilt(this);
			CubeCleanerService.this.now = now;
			return this;
		}

		public Builder withPrefixCube(ByteSequence prefixCube) {
			checkNotBuilt(this);
			CubeCleanerService.this.prefixCube = prefixCube;
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
		return Promise.ofCompletionStage(client.getKVClient().get(cleanupRevisionKey))
			.whenResult(response -> {
				List<KeyValue> kvs = response.getKvs();
				if (kvs.isEmpty()) {
					throw new IllegalStateException("No cleanup revision is found on key '" +
													cleanupRevisionKey + '\'');
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
		return Promise.ofCompletionStage(client.getKVClient().put(cleanupRevisionKey, value))
			.mapException(e -> new CubeException("Failed to update last cleanup revision", e))
			.whenResult(() -> this.lastCleanupRevision = lastCleanupRevision)
			.whenComplete(promiseUpdateLastCleanupRevision.recordStats())
			.toVoid();
	}

	private Watch.Watcher createWatcher() {
		return client.getWatchClient().watch(
			root,
			WatchOption.builder()
				.isPrefix(true)
				.withRevision(lastCleanupRevision + 1L)
				.build(),
			new Watch.Listener() {
				long currentRevision = -1;
				long currentTimestamp = -1;

				final List<Long> currentChunkIds = new ArrayList<>();

				@Override
				public void onNext(WatchResponse response) {
					try {
						for (var event : response.getEvents()) {
							var keyValue = event.getKeyValue();

							long modRevision = keyValue.getModRevision();
							if (modRevision != currentRevision) {
								flushCurrentChunkIds();
								currentRevision = modRevision;
								currentTimestamp = -1;
							}

							var rootKey = keyValue.getKey().substring(root.size());

							if (rootKey.isEmpty()) {
								if (event.getEventType() == EventType.PUT) {
									ByteSequence value = keyValue.getValue();
									currentTimestamp = EtcdUtils.TOUCH_TIMESTAMP_CODEC.decodeValue(value);
								}
								continue;
							}

							if (!rootKey.startsWith(prefixCube)) continue;

							var key = rootKey.substring(prefixCube.size());

							if (event.getEventType() != EventType.DELETE) continue;

							Prefix<String> prefix = aggregationIdCodec.decodePrefix(key);
							long chunkId = chunkIdCodec.decodeKey(prefix.suffix());
							currentChunkIds.add(chunkId);
						}
						flushCurrentChunkIds();
					} catch (MalformedEtcdDataException e) {
						onError(e);
					}
				}

				private void flushCurrentChunkIds() {
					if (currentTimestamp == -1 && !currentChunkIds.isEmpty()) {
						logger.warn("No transaction timestamp found, skip deleting chunks {}", currentChunkIds);
					}
					if (currentChunkIds.isEmpty()) return;

					deletedChunksQueue.add(new DeletedChunksEntry(currentRevision, currentTimestamp, new HashSet<>(currentChunkIds)));
					currentChunkIds.clear();
					reactor.submit(() -> {
						if (cleanupSchedule == null) {
							cleanup();
						}
					});
				}

				boolean recreating;

				@Override
				public void onError(Throwable throwable) {
					logger.warn("Error occurred while watching chunks to be cleaned up", throwable);
					recreate();
				}

				@Override
				public void onCompleted() {
					logger.trace("Watcher completed");
					recreate();
				}

				private void recreate() {
					if (recreating) return;
					recreating = true;

					reactor.submit(() -> {
						if (stopped) return;
						logger.trace("Recreating watcher");

						assert CubeCleanerService.this.watcher != null;
						CubeCleanerService.this.watcher.close();
						CubeCleanerService.this.watcher = createWatcher();
					});
				}
			}
		);
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
		return prefixCube.toString();
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
	public Duration getDefaultCleanupInterval() {
		return Duration.ofMillis(cleanupRetryMillis);
	}

	@JmxAttribute
	public void setCleanupRetryMillis(Duration cleanupRetryMillis) {
		this.cleanupRetryMillis = cleanupRetryMillis.toMillis();
	}

	@JmxAttribute
	public long getLastCleanupRevision() {
		return lastCleanupRevision;
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
	// endregion
}
