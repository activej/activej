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

import io.activej.async.function.AsyncSupplier;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.etcd.jetcd.watch.WatchEvent.EventType.DELETE;
import static io.etcd.jetcd.watch.WatchEvent.EventType.PUT;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class EtcdWatchService extends AbstractReactive {
	private static final SettablePromise<byte[]> UPDATE_CONSUMED = new SettablePromise<>();

	private final Client client;
	private final String key;

	private EtcdWatchService(Reactor reactor, Client client, String key) {
		super(reactor);
		this.client = client;
		this.key = key;
	}

	public static EtcdWatchService create(Reactor reactor, Client client, String key) {
		return new EtcdWatchService(reactor, client, key);
	}

	public AsyncSupplier<byte[]> watch() {
		checkInReactorThread();
		return new AsyncSupplier<>() {
			final AtomicReference<SettablePromise<byte[]>> cbRef = new AtomicReference<>(UPDATE_CONSUMED);
			@Nullable Watcher watcher;

			@Override
			public Promise<byte[]> get() {
				checkInReactorThread();
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
									return Promise.ofException(new IOException("Expected a single key"));

								return Promise.of(kvs.get(0).getValue().getBytes());
							});
				}

				SettablePromise<byte[]> cb = cbRef.get();
				if (cb != UPDATE_CONSUMED && !cb.isComplete()) {
					return Promise.ofException(new IOException("Previous promise has not been completed yet"));
				}

				while (true) {
					if (cb == UPDATE_CONSUMED) {
						SettablePromise<byte[]> newCb = new SettablePromise<>();
						if (cbRef.compareAndSet(UPDATE_CONSUMED, newCb)) {
							reactor.startExternalTask();
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
						onError(new IOException("Unknown event occurred: " + event.getEventType()));
						return;
					}
				}
				if (bs == null) {
					onError(new IOException("Watched key was deleted"));
					return;
				}

				byte[] bytes = bs.getBytes();
				completeCb(cb -> cb.set(bytes));
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

			private void completeCb(Consumer<SettablePromise<byte[]>> consumer) {
				while (true) {
					SettablePromise<byte[]> cb = cbRef.get();
					if (cb == UPDATE_CONSUMED || cb.isComplete()) {
						SettablePromise<byte[]> newCb = new SettablePromise<>();
						consumer.accept(newCb);
						if (cbRef.compareAndSet(cb, newCb)) {
							return;
						}
						continue;
					}

					SettablePromise<byte[]> prevCb = cbRef.getAndSet(UPDATE_CONSUMED);
					assert !prevCb.isComplete();

					reactor.execute(() -> consumer.accept(prevCb));
					reactor.completeExternalTask();
					return;
				}
			}
		};
	}
}
