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
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

import static io.activej.async.util.LogUtils.Level.INFO;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class InMemoryWriteAheadLog<K extends Comparable<K>, S> extends AbstractReactive
		implements IWriteAheadLog<K, S>, ReactiveService {
	private static final Logger logger = LoggerFactory.getLogger(InMemoryWriteAheadLog.class);

	private Map<K, CrdtData<K, S>> map = new TreeMap<>();

	private final CrdtFunction<S> function;
	private final ICrdtStorage<K, S> storage;

	private final AsyncRunnable flush = AsyncRunnables.coalesce(this::doFlush);

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private InMemoryWriteAheadLog(Reactor reactor, CrdtFunction<S> function, ICrdtStorage<K, S> storage) {
		super(reactor);
		this.function = function;
		this.storage = storage;
	}

	public static <K extends Comparable<K>, S> InMemoryWriteAheadLog<K, S> create(Reactor reactor, CrdtFunction<S> function, ICrdtStorage<K, S> storage) {
		return builder(reactor, function, storage).build();
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> InMemoryWriteAheadLog<K, S> create(Reactor reactor, ICrdtStorage<K, S> storage) {
		return builder(reactor, CrdtFunction.ofCrdtType(), storage).build();
	}

	public static <K extends Comparable<K>, S> InMemoryWriteAheadLog<K, S>.Builder builder(Reactor reactor, CrdtFunction<S> function, ICrdtStorage<K, S> storage) {
		return new InMemoryWriteAheadLog<>(reactor, function, storage).new Builder();
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> InMemoryWriteAheadLog<K, S>.Builder builder(Reactor reactor, ICrdtStorage<K, S> storage) {
		return new InMemoryWriteAheadLog<>(reactor, CrdtFunction.ofCrdtType(), storage).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, InMemoryWriteAheadLog<K, S>> {
		private Builder() {}

		public Builder withCurrentTimeProvider(CurrentTimeProvider now) {
			checkNotBuilt(this);
			InMemoryWriteAheadLog.this.now = now;
			return this;
		}

		@Override
		protected InMemoryWriteAheadLog<K, S> doBuild() {
			return InMemoryWriteAheadLog.this;
		}
	}

	@Override
	public Promise<Void> put(K key, S value) {
		checkInReactorThread(this);
		if (logger.isTraceEnabled()) {
			logger.trace("{} value for key {}", map.containsKey(key) ? "Merging" : "Putting new", key);
		}
		doPut(key, new CrdtData<>(key, now.currentTimeMillis(), value));
		return Promise.complete();
	}

	@Override
	public Promise<Void> flush() {
		checkInReactorThread(this);
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
				.then(consumer -> StreamSuppliers.ofIterable(map.values())
						.streamTo(consumer))
				.whenException(e -> map.forEach(this::doPut))
				.whenComplete(toLogger(logger, INFO, INFO, "flush", map.size()));
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		return Promise.complete();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread(this);
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
