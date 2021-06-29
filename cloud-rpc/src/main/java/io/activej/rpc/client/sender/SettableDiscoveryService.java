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
import io.activej.common.Checks;
import io.activej.eventloop.Eventloop;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.Checks.checkState;

public final class SettableDiscoveryService implements DiscoveryService {
	private static final boolean CHECK = Checks.isEnabled(SettableDiscoveryService.class);

	private Map<Object, InetSocketAddress> addressMap;

	private final List<Callback<Map<Object, InetSocketAddress>>> callbacks = new ArrayList<>();

	private SettableDiscoveryService(Map<Object, InetSocketAddress> addressMap) {
		this.addressMap = Collections.unmodifiableMap(new HashMap<>(addressMap));
	}

	public static SettableDiscoveryService create(Map<Object, InetSocketAddress> addressMap) {
		return new SettableDiscoveryService(addressMap);
	}

	@Override
	public void discover(@Nullable Map<Object, InetSocketAddress> previous, Callback<Map<Object, InetSocketAddress>> cb) {
		if (!addressMap.equals(previous)) {
			cb.accept(addressMap, null);
			return;
		}

		callbacks.add(cb);
	}

	public void setAddressMap(Map<Object, InetSocketAddress> addressMap) {
		if (CHECK) {
			checkState(Eventloop.getCurrentEventloop().inEventloopThread());
		}

		if (this.addressMap.equals(addressMap)) return;

		this.addressMap = Collections.unmodifiableMap(new HashMap<>(addressMap));
		List<Callback<Map<Object, InetSocketAddress>>> callbacks = new ArrayList<>(this.callbacks);
		this.callbacks.clear();
		callbacks.forEach(cb -> cb.accept(addressMap, null));
	}
}
