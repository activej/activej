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

import io.activej.async.callback.Callback;
import io.activej.fs.ActiveFs;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public interface DiscoveryService {

	void discover(@Nullable Map<Object, ActiveFs> previous, Callback<Map<Object, ActiveFs>> cb);

	static DiscoveryService constant(Map<Object, ActiveFs> partitions) {
		Map<Object, ActiveFs> constant = Collections.unmodifiableMap(new HashMap<>(partitions));
		return (previous, cb) -> {
			if (!constant.equals(previous)) {
				cb.accept(constant, null);
			}
		};
	}
}
