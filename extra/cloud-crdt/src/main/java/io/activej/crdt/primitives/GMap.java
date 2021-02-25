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

package io.activej.crdt.primitives;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public final class GMap<K, V extends CrdtMergable<V>> implements Map<K, V>, CrdtMergable<GMap<K, V>> {

	private final Map<K, V> map;

	private GMap(Map<K, V> map) {
		this.map = map;
	}

	public GMap() {
		this(new HashMap<>());
	}

	@Override
	public GMap<K, V> merge(GMap<K, V> other) {
		HashMap<K, V> newMap = new HashMap<>(map);
		other.map.forEach((k, v) -> newMap.merge(k, v, CrdtMergable::merge));
		return new GMap<>(newMap);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Nullable
	@Override
	public V put(K key, V value) {
		return map.merge(key, value, CrdtMergable::merge);
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException("GMap is a grow-only map");
	}

	@Override
	public void putAll(@NotNull Map<? extends K, ? extends V> m) {
		m.forEach(this::put);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("GMap is a grow-only map");
	}

	@NotNull
	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException("GMap#keySet is not implemented yet");
	}

	@NotNull
	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException("GMap#values is not implemented yet");
	}

	@NotNull
	@Override
	public Set<Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException("GMap#entrySet is not implemented yet");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		return map.equals(((GMap<?, ?>) o).map);
	}

	@Override
	public int hashCode() {
		return Objects.hash(map);
	}

	@Override
	public String toString() {
		return map.toString();
	}

	public static class Serializer<K, V extends CrdtMergable<V>> implements BinarySerializer<GMap<K, V>> {
		private final BinarySerializer<K> keySerializer;
		private final BinarySerializer<V> valueSerializer;

		public Serializer(BinarySerializer<K> keySerializer, BinarySerializer<V> valueSerializer) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, GMap<K, V> item) {
			out.writeVarInt(item.map.size());
			for (Entry<K, V> entry : item.map.entrySet()) {
				keySerializer.encode(out, entry.getKey());
				valueSerializer.encode(out, entry.getValue());
			}
		}

		@Override
		public GMap<K, V> decode(BinaryInput in) {
			int size = in.readVarInt();
			Map<K, V> map = new HashMap<>(size);
			for (int i = 0; i < size; i++) {
				map.put(keySerializer.decode(in), valueSerializer.decode(in));
			}
			return new GMap<>(map);
		}
	}
}
