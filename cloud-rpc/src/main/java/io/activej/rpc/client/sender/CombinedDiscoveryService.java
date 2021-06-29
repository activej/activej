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

package io.activej.rpc.client.sender;

import io.activej.async.callback.Callback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;

class CombinedDiscoveryService implements DiscoveryService {
	private final Map<DiscoveryService, Map<Object, InetSocketAddress>> discovered = new IdentityHashMap<>();
	private final int discoveryServicesSize;

	private final Set<Callback<Map<Object, InetSocketAddress>>> callbacks = new HashSet<>();

	private Map<Object, InetSocketAddress> totalDiscovered = new HashMap<>();
	private Throwable error;

	CombinedDiscoveryService(List<DiscoveryService> discoveryServices) {
		this.discoveryServicesSize = discoveryServices.size();

		for (DiscoveryService discoveryService : discoveryServices) {
			doDiscover(discoveryService);
		}
	}

	@Override
	public void discover(@Nullable Map<Object, InetSocketAddress> previous, Callback<Map<Object, InetSocketAddress>> cb) {
		if (error != null) {
			cb.accept(null, error);
		}

		if (discovered.size() == discoveryServicesSize && !totalDiscovered.equals(previous)) {
			cb.accept(totalDiscovered, null);
		}

		callbacks.add(cb);
	}

	private void doDiscover(DiscoveryService discoveryService) {
		discoveryService.discover(discovered.get(discoveryService), (result, e) -> {
			if (error != null) return;

			if (e == null) {
				onDiscover(discoveryService, result);
			} else {
				onError(e);
			}
		});
	}

	private void onError(@NotNull Throwable e) {
		error = e;
		completeCallbacks();
	}

	private void onDiscover(DiscoveryService discoveryService, Map<Object, InetSocketAddress> discovered) {
		Map<Object, InetSocketAddress> old = this.discovered.put(discoveryService, discovered);

		Map<Object, InetSocketAddress> newTotalDiscovered = new HashMap<>(totalDiscovered);
		if (old != null) {
			newTotalDiscovered.keySet().removeAll(old.keySet());
		}
		newTotalDiscovered.putAll(discovered);
		this.totalDiscovered = Collections.unmodifiableMap(newTotalDiscovered);

		if (discovered.size() == discoveryServicesSize) {
			completeCallbacks();
		}
	}

	private void completeCallbacks() {
		Set<Callback<Map<Object, InetSocketAddress>>> callbacks = new HashSet<>(this.callbacks);
		this.callbacks.clear();
		if (error != null) {
			callbacks.forEach(cb -> cb.accept(null, error));
		} else {
			callbacks.forEach(cb -> cb.accept(totalDiscovered, error));
		}
	}
}
