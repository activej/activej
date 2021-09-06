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
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface DiscoveryService {

	/**
	 * Discovers actual RPC server addresses
	 * <p>
	 * A previous addresses are supplied. Whenever a set of actual addresses changes
	 * and is not the same as previous addresses, a callback will be completed
	 * with new addresses as a result. A callback will also be completed if an error occurs.
	 * <p>
	 * If there are multiple {@code discover(...)} calls, the callbacks should be completed
	 * in the order in which they were passed to the method
	 *
	 * @param previous previous RPC addresses
	 * @param cb       callback to be completed when a set of actual RPC addresses in changes
	 *                 or an error occurs
	 */
	void discover(@Nullable Map<Object, InetSocketAddress> previous, Callback<Map<Object, InetSocketAddress>> cb);

	static DiscoveryService combined(List<DiscoveryService> discoveryServices) {
		return new CombinedDiscoveryService(discoveryServices);
	}

	/**
	 * A {@code DiscoveryService} that consists of given RPC server addresses that never change
	 *
	 * @param addresses constant RPC server addresses
	 * @return a constant discovery service
	 */
	static DiscoveryService constant(Map<Object, InetSocketAddress> addresses) {
		Map<Object, InetSocketAddress> constantAddresses = Collections.unmodifiableMap(new HashMap<>(addresses));
		return (previous, cb) -> {
			if (!constantAddresses.equals(previous)) {
				cb.accept(constantAddresses, null);
			}
		};
	}
}
