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

import io.activej.async.callback.Callback;
import io.activej.crdt.storage.CrdtStorage;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public interface DiscoveryService<K extends Comparable<K>, S, P extends Comparable<P>> {

	void discover(
			@Nullable Map<P, ? extends CrdtStorage<K, S>> previous,
			Callback<Map<P, ? extends CrdtStorage<K, S>>> cb
	);

	static <K extends Comparable<K>, S, P extends Comparable<P>> DiscoveryService<K, S, P> constant(Map<P, ? extends CrdtStorage<K, S>> partitions) {
		Map<P, ? extends CrdtStorage<K, S>> constant = Collections.unmodifiableMap(new HashMap<>(partitions));
		return (previous, cb) -> {
			if (!constant.equals(previous)) {
				cb.accept(constant, null);
			}
		};
	}
}
