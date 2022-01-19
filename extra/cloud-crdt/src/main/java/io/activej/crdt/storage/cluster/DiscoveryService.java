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
import io.activej.crdt.storage.CrdtStorage;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.sender.RpcStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface DiscoveryService<P> {

	AsyncSupplier<Partitionings<P>> discover();

	interface Partitionings<P> {
		Set<P> getPartitions();

		<K extends Comparable<K>, S> @Nullable Sharder<K> createSharder(Function<P, CrdtStorage<K, S>> provider, List<P> alive);

		<K extends Comparable<K>> RpcStrategy createRpcStrategy(Function<P, @NotNull RpcStrategy> provider, Function<Object, K> keyGetter);
	}

	static <P> DiscoveryService<P> of(Partitionings<P> partitionings) {
		return () -> new AsyncSupplier<Partitionings<P>>() {
			int i = 0;

			@Override
			public Promise<Partitionings<P>> get() {
				return i++ == 0 ? Promise.of(partitionings) : new SettablePromise<>();
			}
		};
	}
}
