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

package io.activej.crdt.hash;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.async.service.ReactiveService;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

public class CrdtMap_Java<K extends Comparable<K>, S> extends AbstractReactive
		implements AsyncCrdtMap<K, S>, ReactiveService {
	private final Map<K, S> map = new TreeMap<>();

	private final BinaryOperator<S> mergeFn;

	private final AsyncRunnable refresh;

	public CrdtMap_Java(Reactor reactor, BinaryOperator<S> mergeFn) {
		super(reactor);
		this.mergeFn = mergeFn;
		this.refresh = Promise::complete;
	}

	public CrdtMap_Java(Reactor reactor, BinaryOperator<S> mergeFn, AsyncCrdtStorage<K, S> storage) {
		super(reactor);
		this.mergeFn = mergeFn;
		this.refresh = AsyncRunnables.reuse(() -> doRefresh(storage));
	}

	@Override
	public Promise<@Nullable S> get(K key) {
		checkInReactorThread();
		return Promise.of(map.get(key));
	}

	@Override
	public Promise<Void> refresh() {
		checkInReactorThread();
		return refresh.run();
	}

	@Override
	public Promise<@Nullable S> put(K key, S value) {
		checkInReactorThread();
		return Promise.of(map.merge(key, value, mergeFn));
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread();
		return refresh();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread();
		return Promise.complete();
	}

	private Promise<Void> doRefresh(AsyncCrdtStorage<K, S> storage) {
		assert storage != null;
		return storage.download()
				.then(supplier -> supplier.streamTo(StreamConsumer.ofConsumer(crdtData -> map.put(crdtData.getKey(), crdtData.getState()))));
	}
}
