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
import io.activej.fs.IFileSystem;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;

import java.util.Map;

/**
 * A service that allows to discover actual {@link IFileSystem} partitions in a cluster
 */
@ComponentInterface
public interface IDiscoveryService {

	/**
	 * Discovers actual {@link IFileSystem} partitions in a cluster
	 * <p>
	 * A previous partitions are supplied. Whenever a set of actual partitions changes
	 * and is not the same as previous partitions, a callback will be completed
	 * with new partitions as a result. A callback will also be completed if an error occurs.
	 * <p>
	 */
	AsyncSupplier<Map<Object, IFileSystem>> discover();

	/**
	 * A {@code DiscoveryService} that consists of given partitions that never change
	 *
	 * @param partitions constant {@link IFileSystem} partitions
	 * @return a constant discovery service
	 */
	static IDiscoveryService constant(Map<Object, IFileSystem> partitions) {
		Map<Object, IFileSystem> constant = Map.copyOf(partitions);
		return () -> new AsyncSupplier<>() {
			int i = 0;

			@Override
			public Promise<Map<Object, IFileSystem>> get() {
				return i++ == 0 ? Promise.of(constant) : new SettablePromise<>();
			}
		};
	}
}
