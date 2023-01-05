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

package io.activej.memcache.client;

import io.activej.promise.Promise;

public interface AsyncMemcacheClient<K, V> {

	Promise<Void> put(K key, V value, int timeout);

	Promise<V> get(K key, int timeout);

	default Promise<Void> put(K key, V value) {
		return put(key, value, Integer.MAX_VALUE);
	}

	default Promise<V> get(K key) {
		return get(key, Integer.MAX_VALUE);
	}
}
