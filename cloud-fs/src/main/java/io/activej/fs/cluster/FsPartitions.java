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

package io.activej.fs.cluster;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.ReactiveService;
import io.activej.common.function.ConsumerEx;
import io.activej.common.initializer.WithInitializer;
import io.activej.fs.ActiveFs;
import io.activej.fs.exception.FsException;
import io.activej.fs.exception.FsIOException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.fs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static java.util.stream.Collectors.toList;

public final class FsPartitions extends AbstractReactive
		implements ReactiveService, WithInitializer<FsPartitions> {
	private static final Logger logger = LoggerFactory.getLogger(FsPartitions.class);

	static final FsException LOCAL_EXCEPTION = new FsException("Local exception");

	private final DiscoveryService discoveryService;

	private final Map<Object, ActiveFs> alivePartitions = new HashMap<>();
	private final Map<Object, ActiveFs> alivePartitionsView = Collections.unmodifiableMap(alivePartitions);

	private final Map<Object, ActiveFs> deadPartitions = new HashMap<>();
	private final Map<Object, ActiveFs> deadPartitionsView = Collections.unmodifiableMap(deadPartitions);

	private final AsyncRunnable checkAllPartitions = AsyncRunnables.reuse(this::doCheckAllPartitions);
	private final AsyncRunnable checkDeadPartitions = AsyncRunnables.reuse(this::doCheckDeadPartitions);

	private final Map<Object, ActiveFs> partitions = new HashMap<>();
	private final Map<Object, ActiveFs> partitionsView = Collections.unmodifiableMap(partitions);

	private ServerSelector serverSelector = RENDEZVOUS_HASH_SHARDER;

	private FsPartitions(Reactor reactor, DiscoveryService discoveryService) {
		super(reactor);
		this.discoveryService = discoveryService;
	}

	public static FsPartitions create(Reactor reactor, DiscoveryService discoveryService) {
		return new FsPartitions(reactor, discoveryService);
	}

	/**
	 * Sets the server selection strategy based on file name and alive partitions
	 */
	public FsPartitions withServerSelector(ServerSelector serverSelector) {
		this.serverSelector = serverSelector;
		return this;
	}

	/**
	 * Returns an unmodifiable view of all partitions
	 */
	public Map<Object, ActiveFs> getPartitions() {
		return partitionsView;
	}

	/**
	 * Returns an unmodifiable view of alive partitions
	 */
	public Map<Object, ActiveFs> getAlivePartitions() {
		return alivePartitionsView;
	}

	/**
	 * Returns an unmodifiable view of dead partitions
	 */
	public Map<Object, ActiveFs> getDeadPartitions() {
		return deadPartitionsView;
	}

	/**
	 * Returns alive {@link ActiveFs} by given id
	 *
	 * @param partitionId id of {@link ActiveFs}
	 * @return alive {@link ActiveFs}
	 */
	public @Nullable ActiveFs get(Object partitionId) {
		return alivePartitions.get(partitionId);
	}

	/**
	 * Starts a check process, which pings all partitions and marks them as dead or alive accordingly
	 *
	 * @return promise of the check
	 */
	public Promise<Void> checkAllPartitions() {
		return checkAllPartitions.run()
				.whenComplete(toLogger(logger, "checkAllPartitions"));
	}

	/**
	 * Starts a check process, which pings all dead partitions to possibly mark them as alive.
	 * This is the preferred method as it does nothing when no partitions are marked as dead,
	 * and RemoteF operations themselves do mark nodes as dead on connection failures.
	 *
	 * @return promise of the check
	 */
	public Promise<Void> checkDeadPartitions() {
		return checkDeadPartitions.run()
				.whenComplete(toLogger(logger, "checkDeadPartitions"));
	}

	/**
	 * Mark a partition as dead. It means that no operations will use it, and it would not be given to the server selector.
	 * Next call to {@link #checkDeadPartitions()} or {@link #checkAllPartitions()} will ping this partition and possibly
	 * mark it as alive again.
	 *
	 * @param partitionId id of the partition to be marked
	 * @param e           optional exception for logging
	 * @return <code>true</code> if partition was alive and <code>false</code> otherwise
	 */
	@SuppressWarnings("UnusedReturnValue")
	public boolean markDead(Object partitionId, @Nullable Exception e) {
		ActiveFs partition = alivePartitions.remove(partitionId);
		if (partition != null) {
			logger.warn("marking {} as dead ", partitionId, e);
			deadPartitions.put(partitionId, partition);
			return true;
		}
		return false;
	}

	public void markAlive(Object partitionId) {
		ActiveFs partition = deadPartitions.remove(partitionId);
		if (partition != null) {
			logger.info("Partition {} is alive again!", partitionId);
			alivePartitions.put(partitionId, partition);
		}
	}

	/**
	 * If partition has returned exception other than {@link FsException} that indicates that there were connection problems
	 * or that there were no response at all
	 */
	public void markIfDead(Object partitionId, Exception e) {
		if (!(e instanceof FsException) || e instanceof FsIOException) {
			markDead(partitionId, e);
		}
	}

	public ConsumerEx<Exception> wrapDeathFn(Object partitionId) {
		return e -> {
			markIfDead(partitionId, e);
			if (e instanceof FsException) {
				throw e;
			}
			logger.warn("Node failed", e);
			throw new FsIOException("Node failed");
		};
	}

	public List<Object> select(String filename) {
		return serverSelector.selectFrom(filename, alivePartitions.keySet());
	}

	public ServerSelector getServerSelector() {
		return serverSelector;
	}

	@Override
	public Promise<?> start() {
		AsyncSupplier<Map<Object, ActiveFs>> discoverySupplier = discoveryService.discover();
		return discoverySupplier.get()
				.whenResult(result -> {
					this.partitions.putAll(result);
					this.alivePartitions.putAll(result);
					checkAllPartitions()
							.whenComplete(() -> rediscover(discoverySupplier));
				});
	}

	@Override
	public Promise<?> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "FsPartitions{partitions=" + partitions + ", deadPartitions=" + deadPartitions + '}';
	}

	private void rediscover(AsyncSupplier<Map<Object, ActiveFs>> discoverySupplier) {
		discoverySupplier.get()
				.whenResult(result -> {
					updatePartitions(result);
					checkAllPartitions()
							.whenComplete(() -> rediscover(discoverySupplier));
				})
				.whenException(e -> {
					logger.warn("Could not discover partitions", e);
					reactor.delayBackground(Duration.ofSeconds(1), () -> rediscover(discoverySupplier));
				});
	}

	private void updatePartitions(Map<Object, ActiveFs> newPartitions) {
		this.partitions.clear();
		this.partitions.putAll(newPartitions);

		alivePartitions.keySet().retainAll(this.partitions.keySet());
		deadPartitions.keySet().retainAll(this.partitions.keySet());

		for (Map.Entry<Object, ActiveFs> entry : this.partitions.entrySet()) {
			Object partitionId = entry.getKey();
			ActiveFs fs = entry.getValue();

			ActiveFs deadFs = deadPartitions.get(partitionId);
			if (deadFs != null) {
				if (deadFs == fs) continue;

				deadPartitions.remove(partitionId);
			}
			alivePartitions.put(partitionId, fs);
		}

		alivePartitions.clear();
		deadPartitions.clear();
	}

	private Promise<Void> doCheckAllPartitions() {
		return Promises.all(
				partitions.entrySet().stream()
						.map(entry -> {
							Object id = entry.getKey();
							return entry.getValue()
									.ping()
									.map(($, e) -> {
										if (e == null) {
											markAlive(id);
										} else {
											markDead(id, e);
										}
										return null;
									});
						}));
	}

	private Promise<Void> doCheckDeadPartitions() {
		return Promises.all(
				deadPartitions.entrySet().stream()
						.map(entry -> entry.getValue()
								.ping()
								.map(($, e) -> {
									if (e == null) {
										markAlive(entry.getKey());
									}
									return null;
								})
						));
	}

	// region JMX
	@JmxAttribute
	public List<String> getAllPartitions() {
		return partitions.keySet().stream()
				.map(Object::toString)
				.collect(toList());
	}
	// endregion
}
