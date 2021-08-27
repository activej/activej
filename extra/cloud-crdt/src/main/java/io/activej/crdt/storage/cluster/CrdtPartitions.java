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

package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.RendezvousHashSharder;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static io.activej.async.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toList;

public final class CrdtPartitions<K extends Comparable<K>, S, P extends Comparable<P>> implements EventloopService, WithInitializer<CrdtPartitions<K, S, P>> {
	private static final Logger logger = LoggerFactory.getLogger(CrdtPartitions.class);

	private final DiscoveryService<K, S, P> discoveryService;

	private final SortedMap<P, CrdtStorage<K, S>> alivePartitions = new TreeMap<>();
	private final Map<P, CrdtStorage<K, S>> alivePartitionsView = Collections.unmodifiableMap(alivePartitions);

	private final SortedMap<P, CrdtStorage<K, S>> deadPartitions = new TreeMap<>();
	private final Map<P, CrdtStorage<K, S>> deadPartitionsView = Collections.unmodifiableMap(deadPartitions);

	private final AsyncSupplier<Void> checkAllPartitions = AsyncSuppliers.reuse(this::doCheckAllPartitions);
	private final AsyncSupplier<Void> checkDeadPartitions = AsyncSuppliers.reuse(this::doCheckDeadPartitions);

	private final SortedMap<P, CrdtStorage<K, S>> partitions = new TreeMap<>();
	private final Map<P, CrdtStorage<K, S>> partitionsView = Collections.unmodifiableMap(partitions);

	private final Eventloop eventloop;

	private RendezvousHashSharder<P> sharder;

	private int topShards = 1;

	private CrdtPartitions(Eventloop eventloop, DiscoveryService<K, S, P> discoveryService) {
		this.eventloop = eventloop;
		this.discoveryService = discoveryService;
	}

	public static <K extends Comparable<K>, S, P extends Comparable<P>> CrdtPartitions<K, S, P> create(Eventloop eventloop, DiscoveryService<K, S, P> discoveryService) {
		return new CrdtPartitions<>(eventloop, discoveryService);
	}

	public CrdtPartitions<K, S, P> withTopShards(int topShards) {
		this.topShards = topShards;
		return this;
	}

	public void setTopShards(int topShards) {
		this.topShards = topShards;
		sharder = RendezvousHashSharder.create(this.alivePartitions.keySet(), topShards);
	}

	public RendezvousHashSharder<P> getSharder() {
		return sharder;
	}

	/**
	 * Returns an unmodifiable view of all partitions
	 */
	public Map<P, CrdtStorage<K, S>> getPartitions() {
		return partitionsView;
	}

	/**
	 * Returns an unmodifiable view of alive partitions
	 */
	public Map<P, CrdtStorage<K, S>> getAlivePartitions() {
		return alivePartitionsView;
	}

	/**
	 * Returns an unmodifiable view of dead partitions
	 */
	public Map<P, CrdtStorage<K, S>> getDeadPartitions() {
		return deadPartitionsView;
	}

	/**
	 * Returns alive {@link CrdtStorage} by given id
	 *
	 * @param partitionId id of {@link CrdtStorage}
	 * @return alive {@link CrdtStorage}
	 */
	@Nullable
	public CrdtStorage<K, S> get(P partitionId) {
		return alivePartitions.get(partitionId);
	}

	/**
	 * Starts a check process, which pings all partitions and marks them as dead or alive accordingly
	 *
	 * @return promise of the check
	 */
	public Promise<Void> checkAllPartitions() {
		return checkAllPartitions.get()
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
		return checkDeadPartitions.get()
				.whenComplete(toLogger(logger, "checkDeadPartitions"));
	}

	/**
	 * Mark partition as dead. It means that no operations will use it and it would not be given to the server selector.
	 * Next call to {@link #checkDeadPartitions()} or {@link #checkAllPartitions()} will ping this partition and possibly
	 * mark it as alive again.
	 *
	 * @param partitionId id of the partition to be marked
	 * @param e           optional exception for logging
	 * @return <code>true</code> if partition was alive and <code>false</code> otherwise
	 */
	@SuppressWarnings("UnusedReturnValue")
	public boolean markDead(P partitionId, @Nullable Throwable e) {
		CrdtStorage<K, S> partition = alivePartitions.remove(partitionId);
		if (partition != null) {
			logger.warn("marking {} as dead ", partitionId, e);
			deadPartitions.put(partitionId, partition);
			recompute();
			return true;
		}
		return false;
	}

	public void markAlive(P partitionId) {
		CrdtStorage<K, S> partition = deadPartitions.remove(partitionId);
		if (partition != null) {
			logger.info("Partition {} is alive again!", partitionId);
			alivePartitions.put(partitionId, partition);
			recompute();
		}
	}

	private void recompute() {
		sharder = RendezvousHashSharder.create(alivePartitions.keySet(), topShards);
	}

	private void rediscover() {
		discoveryService.discover(partitions, (result, e) -> {
			if (e == null) {
				updatePartitions(result);
				recompute();
				checkAllPartitions()
						.whenResult(this::rediscover);
			} else {
				logger.warn("Could not discover partitions", e);
				eventloop.delayBackground(Duration.ofSeconds(1), this::rediscover);
			}
		});
	}

	private void updatePartitions(Map<P, ? extends CrdtStorage<K, S>> newPartitions) {
		partitions.clear();
		partitions.putAll(newPartitions);

		alivePartitions.clear();
		deadPartitions.clear();
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		return Promise.ofCallback(cb ->
				discoveryService.discover(null, (result, e) -> {
					if (e == null) {
						this.partitions.putAll(result);
						this.alivePartitions.putAll(result);
						recompute();

						checkAllPartitions()
								.whenComplete(cb)
								.whenResult(this::rediscover);
					} else {
						cb.setException(e);
					}
				}));
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "CrdtPartitions{partitions=" + partitions + ", deadPartitions=" + deadPartitions + '}';
	}

	private Promise<Void> doCheckAllPartitions() {
		return Promises.all(
				partitions.entrySet().stream()
						.map(entry -> {
							P id = entry.getKey();
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
								})));
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
