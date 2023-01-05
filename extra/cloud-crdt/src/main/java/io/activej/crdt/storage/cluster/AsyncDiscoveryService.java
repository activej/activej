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
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.sender.RpcStrategy;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface AsyncDiscoveryService<P> {

	AsyncSupplier<PartitionScheme<P>> discover();

	interface PartitionScheme<P> {
		Set<P> getPartitions();

		AsyncCrdtStorage<?, ?> provideCrdtConnection(P partition);

		RpcStrategy provideRpcConnection(P partition);

		<K extends Comparable<K>> @Nullable Sharder<K> createSharder(List<P> alive);

		<K extends Comparable<K>> RpcStrategy createRpcStrategy(Function<Object, K> keyGetter);

		boolean isReadValid(Collection<P> alive);
	}

	static <P> AsyncDiscoveryService<P> of(PartitionScheme<P> partitionScheme) {
		return () -> new AsyncSupplier<>() {
			int i = 0;

			@Override
			public Promise<PartitionScheme<P>> get() {
				return i++ == 0 ? Promise.of(partitionScheme) : new SettablePromise<>();
			}
		};
	}
}
