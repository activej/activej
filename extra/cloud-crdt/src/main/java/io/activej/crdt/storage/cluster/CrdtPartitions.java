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
import io.activej.common.api.WithInitializer;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.RendezvousHashSharder;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Utils.parseInetSocketAddress;
import static io.activej.common.collection.CollectionUtils.difference;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class CrdtPartitions<K extends Comparable<K>, S> implements EventloopService, WithInitializer<CrdtPartitions<K, S>> {
	private static final Logger logger = LoggerFactory.getLogger(CrdtPartitions.class);

	private final SortedMap<Comparable<?>, CrdtStorage<K, S>> alivePartitions = new TreeMap<>();
	private final Map<Comparable<?>, CrdtStorage<K, S>> alivePartitionsView = Collections.unmodifiableMap(alivePartitions);

	private final SortedMap<Comparable<?>, CrdtStorage<K, S>> deadPartitions = new TreeMap<>();
	private final Map<Comparable<?>, CrdtStorage<K, S>> deadPartitionsView = Collections.unmodifiableMap(deadPartitions);

	private final AsyncSupplier<Void> checkAllPartitions = AsyncSuppliers.reuse(this::doCheckAllPartitions);
	private final AsyncSupplier<Void> checkDeadPartitions = AsyncSuppliers.reuse(this::doCheckDeadPartitions);

	private final SortedMap<Comparable<?>, CrdtStorage<K, S>> partitions = new TreeMap<>();
	private final Map<Comparable<?>, CrdtStorage<K, S>> partitionsView = Collections.unmodifiableMap(partitions);

	private final Eventloop eventloop;

	private RendezvousHashSharder sharder;

	private CrdtDataSerializer<K, S> serializer;
	private int topShards = 1;

	private CrdtPartitions(Eventloop eventloop, Map<? extends Comparable<?>, ? extends CrdtStorage<K, S>> partitions) {
		this.eventloop = eventloop;
		this.partitions.putAll(partitions);
		this.alivePartitions.putAll(partitions);
		this.sharder = RendezvousHashSharder.create(this.partitions.keySet(), topShards);
	}

	public static <K extends Comparable<K>, S> CrdtPartitions<K, S> create(Eventloop eventloop) {
		return new CrdtPartitions<>(eventloop, Collections.emptyMap());
	}

	public static <K extends Comparable<K>, S> CrdtPartitions<K, S> create(Eventloop eventloop, Map<? extends Comparable<?>, ? extends CrdtStorage<K, S>> partitions) {
		return new CrdtPartitions<>(eventloop, partitions);
	}

	public CrdtPartitions<K, S> withPartition(Comparable<?> id, CrdtStorage<K, S> partition) {
		this.partitions.put(id, partition);
		alivePartitions.put(id, partition);
		return this;
	}

	public CrdtPartitions<K, S> withSerializer(@NotNull CrdtDataSerializer<K, S> serializer) {
		this.serializer = serializer;
		return this;
	}

	public void setTopShards(int topShards) {
		this.topShards = topShards;
		sharder = RendezvousHashSharder.create(this.alivePartitions.keySet(), topShards);
	}

	public RendezvousHashSharder getSharder() {
		return sharder;
	}

	/**
	 * Returns an unmodifiable view of all partitions
	 */
	public Map<Comparable<?>, CrdtStorage<K, S>> getPartitions() {
		return partitionsView;
	}

	/**
	 * Returns an unmodifiable view of alive partitions
	 */
	public Map<Comparable<?>, CrdtStorage<K, S>> getAlivePartitions() {
		return alivePartitionsView;
	}

	/**
	 * Returns an unmodifiable view of dead partitions
	 */
	public Map<Comparable<?>, CrdtStorage<K, S>> getDeadPartitions() {
		return deadPartitionsView;
	}

	/**
	 * Returns alive {@link CrdtStorage} by given id
	 *
	 * @param partitionId id of {@link CrdtStorage}
	 * @return alive {@link CrdtStorage}
	 */
	@Nullable
	public CrdtStorage<K, S> get(Comparable<?> partitionId) {
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
	public boolean markDead(Comparable<?> partitionId, @Nullable Throwable e) {
		CrdtStorage<K, S> partition = alivePartitions.remove(partitionId);
		if (partition != null) {
			logger.warn("marking {} as dead ", partitionId, e);
			deadPartitions.put(partitionId, partition);
			recompute();
			return true;
		}
		return false;
	}

	public void markAlive(Comparable<?> partitionId) {
		CrdtStorage<K, S> partition = deadPartitions.remove(partitionId);
		if (partition != null) {
			logger.info("Partition {} is alive again!", partitionId);
			alivePartitions.put(partitionId, partition);
			recompute();
		}
	}

	public int[] shard(Object key) {
		return sharder.shard(key);
	}

	private void recompute() {
		sharder = RendezvousHashSharder.create(alivePartitions.keySet(), topShards);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<?> start() {
		return checkAllPartitions();
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
							Comparable<?> id = entry.getKey();
							return entry.getValue()
									.ping()
									.mapEx(($, e) -> {
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
								.mapEx(($, e) -> {
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

	@JmxOperation
	public void setPartitions(List<String> partitions) throws MalformedDataException {
		Map<String, Comparable<?>> previousPartitions = this.partitions.keySet().stream()
				.collect(toMap(Object::toString, Function.identity()));
		Set<String> previousPartitionsKeyset = previousPartitions.keySet();
		logger.info("Setting new partitions. Previous partitions: {}, new partitions: {}", previousPartitionsKeyset, partitions);
		Set<String> partitionsSet = new HashSet<>(partitions);
		for (String toRemove : difference(previousPartitionsKeyset, partitionsSet)) {
			Comparable<?> key = previousPartitions.get(toRemove);
			this.partitions.remove(key);
			this.alivePartitions.remove(key);
			this.deadPartitions.remove(key);
		}

		Set<String> toAdd = difference(partitionsSet, previousPartitionsKeyset);
		if (!toAdd.isEmpty() && serializer == null) {
			throw new IllegalStateException("No serializer set, cannot add partitions");
		}
		for (String address : toAdd) {
			CrdtStorageClient<K, S> client = CrdtStorageClient.create(eventloop, parseInetSocketAddress(address), serializer);
			this.partitions.put(address, client);
			this.alivePartitions.put(address, client);
		}
	}
	// endregion
}
