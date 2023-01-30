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
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

import static io.activej.reactor.Reactive.checkInReactorThread;

public class JavaCrdtMap<K extends Comparable<K>, S> extends AbstractReactive
		implements ICrdtMap<K, S>, ReactiveService {
	private final Map<K, S> map = new TreeMap<>();

	private final BinaryOperator<S> mergeFn;

	private final AsyncRunnable refresh;

	public JavaCrdtMap(Reactor reactor, BinaryOperator<S> mergeFn) {
		super(reactor);
		this.mergeFn = mergeFn;
		this.refresh = Promise::complete;
	}

	public JavaCrdtMap(Reactor reactor, BinaryOperator<S> mergeFn, ICrdtStorage<K, S> storage) {
		super(reactor);
		this.mergeFn = mergeFn;
		this.refresh = AsyncRunnables.reuse(() -> doRefresh(storage));
	}

	@Override
	public Promise<@Nullable S> get(K key) {
		checkInReactorThread(this);
		return Promise.of(map.get(key));
	}

	@Override
	public Promise<Void> refresh() {
		checkInReactorThread(this);
		return refresh.run();
	}

	@Override
	public Promise<@Nullable S> put(K key, S value) {
		checkInReactorThread(this);
		return Promise.of(map.merge(key, value, mergeFn));
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		return refresh();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread(this);
		return Promise.complete();
	}

	private Promise<Void> doRefresh(ICrdtStorage<K, S> storage) {
		assert storage != null;
		return storage.download()
				.then(supplier -> supplier.streamTo(StreamConsumer.ofConsumer(crdtData -> map.put(crdtData.getKey(), crdtData.getState()))));
	}
}
