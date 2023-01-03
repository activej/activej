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

package io.activej.crdt.wal;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.async.service.ReactiveService;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

import static io.activej.async.util.LogUtils.Level.INFO;
import static io.activej.async.util.LogUtils.toLogger;

public class InMemoryWriteAheadLog<K extends Comparable<K>, S> extends AbstractReactive
		implements WriteAheadLog<K, S>, ReactiveService, WithInitializer<InMemoryWriteAheadLog<K, S>> {
	private static final Logger logger = LoggerFactory.getLogger(InMemoryWriteAheadLog.class);

	private Map<K, CrdtData<K, S>> map = new TreeMap<>();

	private final CrdtFunction<S> function;
	private final CrdtStorage<K, S> storage;

	private final AsyncRunnable flush = AsyncRunnables.coalesce(this::doFlush);

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private InMemoryWriteAheadLog(Reactor reactor, CrdtFunction<S> function, CrdtStorage<K, S> storage) {
		super(reactor);
		this.function = function;
		this.storage = storage;
	}

	public static <K extends Comparable<K>, S> InMemoryWriteAheadLog<K, S> create(Reactor reactor, CrdtFunction<S> function, CrdtStorage<K, S> storage) {
		return new InMemoryWriteAheadLog<>(reactor, function, storage);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> InMemoryWriteAheadLog<K, S> create(Reactor reactor, CrdtStorage<K, S> storage) {
		return new InMemoryWriteAheadLog<>(reactor, CrdtFunction.ofCrdtType(), storage);
	}

	public InMemoryWriteAheadLog<K, S> withCurrentTimeProvider(CurrentTimeProvider now) {
		this.now = now;
		return this;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		if (logger.isTraceEnabled()) {
			logger.trace("{} value for key {}", map.containsKey(key) ? "Merging" : "Putting new", key);
		}
		doPut(key, new CrdtData<>(key, now.currentTimeMillis(), value));
		return Promise.complete();
	}

	@Override
	public Promise<Void> flush() {
		return flush.run();
	}

	private Promise<Void> doFlush() {
		if (map.isEmpty()) {
			logger.info("Nothing to flush");
			return Promise.complete();
		}

		Map<K, CrdtData<K, S>> map = this.map;
		this.map = new TreeMap<>();

		return storage.upload()
				.then(consumer -> StreamSupplier.ofIterable(map.values())
						.streamTo(consumer))
				.whenException(e -> map.forEach(this::doPut))
				.whenComplete(toLogger(logger, INFO, INFO, "flush", map.size()));
	}

	@Override
	public Promise<?> start() {
		return Promise.complete();
	}

	@Override
	public Promise<?> stop() {
		return flush();
	}

	private void doPut(K key, CrdtData<K, S> value) {
		map.merge(key, value, (a, b) -> {
			long timestamp = Math.max(a.getTimestamp(), b.getTimestamp());
			S merged = function.merge(a.getState(), a.getTimestamp(), b.getState(), b.getTimestamp());
			return new CrdtData<>(key, timestamp, merged);
		});
	}
}
