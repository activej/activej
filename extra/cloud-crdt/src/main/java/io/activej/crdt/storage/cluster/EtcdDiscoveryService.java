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
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.CrdtException;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.types.TypeT;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.activej.crdt.util.Utils.fromJson;
import static io.etcd.jetcd.watch.WatchEvent.EventType.DELETE;
import static io.etcd.jetcd.watch.WatchEvent.EventType.PUT;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EtcdDiscoveryService implements DiscoveryService<PartitionId> {
	private static final SettablePromise<PartitionScheme<PartitionId>> UPDATE_CONSUMED = new SettablePromise<>();
	private static final TypeT<List<RendezvousPartitionGroup<PartitionId>>> PARTITION_GROUPS_TYPE = new TypeT<List<RendezvousPartitionGroup<PartitionId>>>() {};

	private final Eventloop eventloop;
	private final Client client;
	private final String key;

	private @Nullable Function<PartitionId, @NotNull RpcStrategy> rpcProvider;
	private @Nullable Function<PartitionId, @NotNull CrdtStorage<?, ?>> crdtProvider;

	private EtcdDiscoveryService(Eventloop eventloop, Client client, String key) {
		this.eventloop = eventloop;
		this.client = client;
		this.key = key;
	}

	public static EtcdDiscoveryService create(Eventloop eventloop, Client client, String key) {
		return new EtcdDiscoveryService(eventloop, client, key);
	}

	public EtcdDiscoveryService withCrdtProvider(Function<PartitionId, CrdtStorage<?, ?>> crdtProvider) {
		this.crdtProvider = crdtProvider;
		return this;
	}

	public EtcdDiscoveryService withRpcProvider(Function<PartitionId, RpcStrategy> rpcProvider) {
		this.rpcProvider = rpcProvider;
		return this;
	}

	@Override
	public AsyncSupplier<PartitionScheme<PartitionId>> discover() {
		return new AsyncSupplier<PartitionScheme<PartitionId>>() {
			final AtomicReference<SettablePromise<PartitionScheme<PartitionId>>> cbRef = new AtomicReference<>(UPDATE_CONSUMED);
			@Nullable Watcher watcher;

			@Override
			public Promise<PartitionScheme<PartitionId>> get() {
				if (watcher == null) {
					ByteSequence bsKey = ByteSequence.from(key, UTF_8);
					watcher = client.getWatchClient().watch(bsKey, this::onResponse, this::onError);
					KV kvClient = client.getKVClient();
					return Promise.ofCompletionStage(kvClient.get(bsKey))
							.whenComplete(kvClient::close)
							.then(getResponse -> {
								List<KeyValue> kvs = getResponse.getKvs();
								if (kvs.isEmpty()) return get();

								if (kvs.size() > 1)
									return Promise.ofException(new CrdtException("Expected a single key"));

								return Promise.of(parseScheme(kvs.get(0).getValue()));
							});
				}

				SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.get();
				if (cb != UPDATE_CONSUMED && !cb.isComplete()) {
					return Promise.ofException(new CrdtException("Previous promise has not been completed yet"));
				}

				while (true) {
					if (cb == UPDATE_CONSUMED) {
						SettablePromise<PartitionScheme<PartitionId>> newCb = new SettablePromise<>();
						if (cbRef.compareAndSet(UPDATE_CONSUMED, newCb)) {
							eventloop.startExternalTask();
							return newCb;
						}
						cb = cbRef.get();
						continue;
					}

					return cbRef.getAndSet(UPDATE_CONSUMED);
				}
			}

			private void onResponse(WatchResponse watchResponse) {
				List<WatchEvent> events = watchResponse.getEvents();
				if (events.isEmpty()) return;

				ByteSequence bs = null;
				for (WatchEvent event : events) {
					if (event.getEventType() == PUT) {
						bs = event.getKeyValue().getValue();
					} else if (event.getEventType() == DELETE) {
						bs = null;
					} else {
						onError(new CrdtException("Unknown event occurred: " + event.getEventType()));
						return;
					}
				}
				if (bs == null) {
					onError(new CrdtException("Watched key was deleted"));
					return;
				}

				try {
					PartitionScheme<PartitionId> partitionScheme = parseScheme(bs);
					completeCb(cb -> cb.set(partitionScheme));
				} catch (MalformedDataException e) {
					onError(new CrdtException("Could not parse file content", e));
				}
			}

			private void onError(Throwable error) {
				Exception exception;
				if (error instanceof Exception) {
					exception = (Exception) error;
				} else {
					exception = new RuntimeException("Fatal error", error);
				}
				completeCb(cb -> cb.setException(exception));
			}

			private void completeCb(Consumer<SettablePromise<PartitionScheme<PartitionId>>> consumer) {
				while (true) {
					SettablePromise<PartitionScheme<PartitionId>> cb = cbRef.get();
					if (cb == UPDATE_CONSUMED || cb.isComplete()) {
						SettablePromise<PartitionScheme<PartitionId>> newCb = new SettablePromise<>();
						consumer.accept(newCb);
						if (cbRef.compareAndSet(cb, newCb)) {
							return;
						}
						continue;
					}

					SettablePromise<PartitionScheme<PartitionId>> prevCb = cbRef.getAndSet(UPDATE_CONSUMED);
					assert !prevCb.isComplete();

					eventloop.execute(() -> consumer.accept(prevCb));
					eventloop.completeExternalTask();
					return;
				}
			}
		};
	}

	private RendezvousPartitionScheme<PartitionId> parseScheme(ByteSequence bs) throws MalformedDataException {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = fromJson(PARTITION_GROUPS_TYPE, bs.getBytes());
		RendezvousPartitionScheme<PartitionId> scheme = RendezvousPartitionScheme.create(partitionGroups)
				.withPartitionIdGetter(PartitionId::getId);

		if (rpcProvider != null) scheme.withRpcProvider(rpcProvider);
		if (crdtProvider != null) scheme.withCrdtProvider(crdtProvider);

		return scheme;
	}
}
