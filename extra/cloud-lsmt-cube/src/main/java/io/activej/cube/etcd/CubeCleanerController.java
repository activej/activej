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
import io.activej.jmx.api.attribute.JmxOperation;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

import static io.activej.async.function.AsyncRunnables.reuse;
import static io.activej.cube.etcd.EtcdUtils.CHUNK_ID_CODEC;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CubeCleanerController extends AbstractReactive
	implements ReactiveService, ReactiveJmxBean {

	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private static final ByteSequence CUBE = byteSequenceFrom("cube.");
	private static final EtcdPrefixCodec<String> AGGREGATION_ID_CODEC = EtcdPrefixCodecs.ofTerminatingString('.');
	private static final EtcdValueCodec<Long> REVISION_CODEC = EtcdValueCodecs.ofLongString();

	public static final Duration DEFAULT_CLEANUP_OLDER_THAN = ApplicationSettings.getDuration(CubeCleanerController.class, "cleanupOlderThan", Duration.ofHours(1));
	public static final Duration DEFAULT_CLEANUP_INTERVAL = ApplicationSettings.getDuration(CubeCleanerController.class, "cleanupOlderThan", Duration.ofMinutes(1));

	private final Client client;
	private final AggregationChunkStorage<Long> storage;
	private final ByteSequence root;
	private final ByteSequence cleanupRevisionKey;

	private final Queue<DeletedChunkEntry> deletedChunksQueue = new PriorityBlockingQueue<>();

	private final AsyncRunnable cleanup = reuse(this::doCleanup);

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private EtcdKeyCodec<Long> chunkIdCodec = CHUNK_ID_CODEC;

	private ByteSequence prefixCube = CUBE;

	private @Nullable Watch.Watcher watcher;

	private long lastCleanupRevision;

	private long cleanupOlderThanMillis = DEFAULT_CLEANUP_OLDER_THAN.toMillis();
	private long defaultCleanupIntervalMillis = DEFAULT_CLEANUP_INTERVAL.toMillis();

	private volatile boolean stopped;
	private @Nullable ScheduledRunnable cleanupSchedule;

	// region JMX
	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseDeleteChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseUpdateLastCleanupRevision = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private CurrentTimeProvider now = reactor;

	private CubeCleanerController(Client client, AggregationChunkStorage<Long> storage, ByteSequence root, ByteSequence cleanupRevisionKey) {
		super(storage.getReactor());
		this.client = client;
		this.storage = storage;
		this.root = root;
		this.cleanupRevisionKey = cleanupRevisionKey;
	}

	public static CubeCleanerController create(Client client, AggregationChunkStorage<Long> storage, ByteSequence root, ByteSequence cleanupRevisionKey) {
		return builder(client, storage, root, cleanupRevisionKey).build();
	}

	public static Builder builder(Client client, AggregationChunkStorage<Long> storage, ByteSequence root, ByteSequence cleanupRevisionKey) {
		return new CubeCleanerController(client, storage, root, cleanupRevisionKey).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeCleanerController> {
		private Builder() {}

		public Builder withCurrentTimeProvider(CurrentTimeProvider now) {
			checkNotBuilt(this);
			CubeCleanerController.this.now = now;
			return this;
		}

		public Builder withPrefixCube(ByteSequence prefixCube) {
			checkNotBuilt(this);
			CubeCleanerController.this.prefixCube = prefixCube;
			return this;
		}

		public Builder withCleanupOlderThen(Duration cleanupOlderThan) {
			checkNotBuilt(this);
			CubeCleanerController.this.cleanupOlderThanMillis = cleanupOlderThan.toMillis();
			return this;
		}

		public Builder withDefaultCleanupInterval(Duration defaultCleanupInterval) {
			checkNotBuilt(this);
			CubeCleanerController.this.defaultCleanupIntervalMillis = defaultCleanupInterval.toMillis();
			return this;
		}

		public Builder withAggregationIdCodec(EtcdPrefixCodec<String> aggregationIdCodec) {
			checkNotBuilt(this);
			CubeCleanerController.this.aggregationIdCodec = aggregationIdCodec;
			return this;
		}

		public Builder withChunkIdCodec(EtcdKeyCodec<Long> chunkIdCodec) {
			checkNotBuilt(this);
			CubeCleanerController.this.chunkIdCodec = chunkIdCodec;
			return this;
		}

		@Override
		protected CubeCleanerController doBuild() {
			return CubeCleanerController.this;
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

	public Promise<Void> cleanup() {
		checkInReactorThread(reactor);
		return cleanup.run();
	}

	private Promise<Void> doCleanup() {
		checkInReactorThread(reactor);
		if (stopped) return Promise.complete();

		if (cleanupSchedule != null) {
			cleanupSchedule.cancel();
			cleanupSchedule = null;
		}

		long cleanupStartTimestamp = now.currentTimeMillis();

		long nextCleanupTimestamp = cleanupStartTimestamp + defaultCleanupIntervalMillis;
		long lastCleanupRevision = -1;
		List<DeletedChunkEntry> chunkEntriesToCleanup = new ArrayList<>();

		while (true) {
			DeletedChunkEntry chunkEntry = deletedChunksQueue.peek();
			if (chunkEntry == null) break;

			if (chunkEntry.deleteTimestamp() + cleanupOlderThanMillis > cleanupStartTimestamp) {
				nextCleanupTimestamp = chunkEntry.deleteTimestamp + cleanupOlderThanMillis + 1L;
				break;
			}

			deletedChunksQueue.remove();
			chunkEntriesToCleanup.add(chunkEntry);
			lastCleanupRevision = chunkEntry.deleteRevision;
		}

		if (chunkEntriesToCleanup.isEmpty()) {
			logger.trace("No chunks to be cleaned up");
			cleanupSchedule = reactor.scheduleBackground(nextCleanupTimestamp, this::cleanup);
			return Promise.complete();
		}

		Set<Long> chunksToCleanup = chunkEntriesToCleanup.stream()
			.map(DeletedChunkEntry::chunkId)
			.collect(Collectors.toSet());

		return doCleanup(chunksToCleanup, nextCleanupTimestamp, lastCleanupRevision)
			.whenException($ -> deletedChunksQueue.addAll(chunkEntriesToCleanup));
	}

	private Promise<Void> doCleanup(Set<Long> chunksToCleanup, long nextCleanupTimestamp, long lastCleanupRevision) {
		logger.trace("Chunks to be cleaned up: {}", chunksToCleanup);

		return deleteChunksFromStorage(chunksToCleanup)
			.then(() -> updateLastCleanupRevision(lastCleanupRevision))
			.whenResult(() -> {
				logger.trace("Chunks successfully cleaned up");
				cleanupSchedule = reactor.scheduleBackground(nextCleanupTimestamp, this::cleanup);
			})
			.whenException(e -> {
				logger.warn("Failed to cleanup chunks", e);
				long nextCleanupAt = now.currentTimeMillis() + defaultCleanupIntervalMillis;
				cleanupSchedule = reactor.scheduleBackground(nextCleanupAt, this::cleanup);
			})
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
					for (Long currentChunkId : currentChunkIds) {
						DeletedChunkEntry entry = new DeletedChunkEntry(currentChunkId, currentRevision, currentTimestamp);
						deletedChunksQueue.add(entry);
					}
					currentChunkIds.clear();
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

						assert CubeCleanerController.this.watcher != null;
						CubeCleanerController.this.watcher.close();
						CubeCleanerController.this.watcher = createWatcher();
					});
				}
			}
		);
	}

	public record DeletedChunkEntry(long chunkId, long deleteRevision, long deleteTimestamp)
		implements Comparable<DeletedChunkEntry> {

		@Override
		public int compareTo(@NotNull CubeCleanerController.DeletedChunkEntry o) {
			return Long.compare(deleteTimestamp, o.deleteTimestamp);
		}
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
	}

	@JmxAttribute
	public Duration getDefaultCleanupInterval() {
		return Duration.ofMillis(defaultCleanupIntervalMillis);
	}

	@JmxAttribute
	public void setDefaultCleanupInterval(Duration defaultCleanupInterval) {
		this.defaultCleanupIntervalMillis = defaultCleanupInterval.toMillis();
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

	@JmxOperation
	public void cleanupNow() {
		cleanup();
	}
	// endregion
}
