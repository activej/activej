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

package io.activej.remotefs.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.common.api.WithInitializer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.remotefs.FsClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.remotefs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;

public final class FsPartitions implements EventloopService, WithInitializer<FsPartitions> {
	private static final Logger logger = LoggerFactory.getLogger(FsPartitions.class);

	private final Map<Object, FsClient> aliveClients = new HashMap<>();
	private final Map<Object, FsClient> aliveClientsView = Collections.unmodifiableMap(aliveClients);

	private final Map<Object, FsClient> deadClients = new HashMap<>();
	private final Map<Object, FsClient> deadClientsView = Collections.unmodifiableMap(deadClients);

	private final AsyncSupplier<Void> checkAllPartitions = AsyncSuppliers.reuse(this::doCheckAllPartitions);
	private final AsyncSupplier<Void> checkDeadPartitions = AsyncSuppliers.reuse(this::doCheckDeadPartitions);

	private final Map<Object, FsClient> clients;
	private final Map<Object, FsClient> clientsView;

	private final Eventloop eventloop;

	private ServerSelector serverSelector = RENDEZVOUS_HASH_SHARDER;

	private FsPartitions(Eventloop eventloop, Map<Object, FsClient> clients) {
		this.eventloop = eventloop;
		this.clients = clients;
		this.aliveClients.putAll(clients);
		this.clientsView = Collections.unmodifiableMap(clients);
	}

	public static FsPartitions create(Eventloop eventloop) {
		return new FsPartitions(eventloop, new HashMap<>());
	}

	public static FsPartitions create(Eventloop eventloop, Map<Object, FsClient> clients) {
		return new FsPartitions(eventloop, clients);
	}

	public FsPartitions withPartition(Object id, FsClient client) {
		clients.put(id, client);
		aliveClients.put(id, client);
		return this;
	}

	/**
	 * Sets the server selection strategy based on file name and alive partitions
	 */
	public FsPartitions withServerSelector(@NotNull ServerSelector serverSelector) {
		this.serverSelector = serverSelector;
		return this;
	}

	/**
	 * Returns an unmodifiable view of all clients
	 */
	public Map<Object, FsClient> getClients() {
		return clientsView;
	}

	/**
	 * Returns an unmodifiable view of alive clients
	 */
	public Map<Object, FsClient> getAliveClients() {
		return aliveClientsView;
	}

	/**
	 * Returns an unmodifiable view of dead clients
	 */
	public Map<Object, FsClient> getDeadClients() {
		return deadClientsView;
	}

	/**
	 * Returns alive {@link FsClient} by given id
	 *
	 * @param partitionId id of {@link FsClient}
	 * @return alive {@link FsClient}
	 */
	@Nullable
	public FsClient get(Object partitionId) {
		return aliveClients.get(partitionId);
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
	 * This is the preferred method as it does nothing when no clients are marked as dead,
	 * and RemoteFS operations themselves do mark nodes as dead on connection failures.
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
	public boolean markDead(Object partitionId, @Nullable Throwable e) {
		FsClient client = aliveClients.remove(partitionId);
		if (client != null) {
			logger.warn("marking {} as dead ", partitionId, e);
			deadClients.put(partitionId, client);
			return true;
		}
		return false;
	}

	public void markAlive(Object partitionId) {
		FsClient client = deadClients.remove(partitionId);
		if (client != null) {
			logger.info("Partition {} is alive again!", partitionId);
			aliveClients.put(partitionId, client);
		}
	}

	public List<Object> select(String filename) {
		return serverSelector.selectFrom(filename, aliveClients.keySet());
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public ServerSelector getServerSelector() {
		return serverSelector;
	}

	@NotNull
	@Override
	public  Promise<?> start() {
		return checkAllPartitions();
	}

	@NotNull
	@Override
	public Promise<?> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "FsPartitions{clients=" + clients + ", deadClients=" + deadClients + '}';
	}

	private Promise<Void> doCheckAllPartitions() {
		return Promises.all(
				clients.entrySet().stream()
						.map(entry -> {
							Object id = entry.getKey();
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
				deadClients.entrySet().stream()
						.map(entry -> entry.getValue()
								.ping()
								.mapEx(($, e) -> {
									if (e == null) {
										markAlive(entry.getKey());
									}
									return null;
								})));
	}
}
