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
import io.activej.common.annotation.ComponentInterface;
import io.activej.fs.AsyncFileSystem;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;

import java.util.Map;

/**
 * A service that allows to discover actual {@link AsyncFileSystem} partitions in a cluster
 */
@ComponentInterface
public interface AsyncDiscoveryService {

	/**
	 * Discovers actual {@link AsyncFileSystem} partitions in a cluster
	 * <p>
	 * A previous partitions are supplied. Whenever a set of actual partitions changes
	 * and is not the same as previous partitions, a callback will be completed
	 * with new partitions as a result. A callback will also be completed if an error occurs.
	 * <p>
	 */
	AsyncSupplier<Map<Object, AsyncFileSystem>> discover();

	/**
	 * A {@code DiscoveryService} that consists of given partitions that never change
	 *
	 * @param partitions constant {@link AsyncFileSystem} partitions
	 * @return a constant discovery service
	 */
	static AsyncDiscoveryService constant(Map<Object, AsyncFileSystem> partitions) {
		Map<Object, AsyncFileSystem> constant = Map.copyOf(partitions);
		return () -> new AsyncSupplier<>() {
			int i = 0;

			@Override
			public Promise<Map<Object, AsyncFileSystem>> get() {
				return i++ == 0 ? Promise.of(constant) : new SettablePromise<>();
			}
		};
	}
}
