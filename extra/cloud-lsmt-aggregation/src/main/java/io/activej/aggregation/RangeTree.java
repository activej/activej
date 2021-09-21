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

package io.activej.aggregation;

import io.activej.common.initializer.WithInitializer;

import java.util.*;

import static java.util.Collections.*;

public final class RangeTree<K, V> implements WithInitializer<RangeTree<K, V>> {

	public static final class Segment<V> {
		private final LinkedHashSet<V> set = new LinkedHashSet<>();
		private final LinkedHashSet<V> closing = new LinkedHashSet<>();

		public Set<V> getSet() {
			return unmodifiableSet(set);
		}

		public Set<V> getClosingSet() {
			return unmodifiableSet(closing);
		}

		private static <V> Segment<V> cloneOf(Segment<V> segment) {
			Segment<V> result = new Segment<>();
			result.set.addAll(segment.set);
			result.closing.addAll(segment.closing);
			return result;
		}

		@Override
		public String toString() {
			return set + "; " + closing;
		}
	}

	private final TreeMap<K, Segment<V>> segments;

	public SortedMap<K, Segment<V>> getSegments() {
		return unmodifiableSortedMap(segments);
	}

	private RangeTree(TreeMap<K, Segment<V>> segments) {
		this.segments = segments;
	}

	public static <K extends Comparable<K>, V> RangeTree<K, V> create() {
		return new RangeTree<>(new TreeMap<>());
	}

	public static <K extends Comparable<K>, V> RangeTree<K, V> cloneOf(RangeTree<K, V> source) {
		RangeTree<K, V> result = create();
		result.segments.putAll(source.segments);
		for (Map.Entry<K, Segment<V>> entry : result.segments.entrySet()) {
			entry.setValue(Segment.cloneOf(entry.getValue()));
		}
		return result;
	}

	private Segment<V> ensureSegment(K key) {
		return segments.computeIfAbsent(key, $ -> {
			Map.Entry<K, Segment<V>> prevEntry = segments.lowerEntry(key);
			Segment<V> segment = new Segment<>();
			if (prevEntry != null) {
				segment.set.addAll(prevEntry.getValue().set);
			}
			return segment;
		});
	}

	public void put(K lower, K upper, V value) {
		ensureSegment(lower);
		ensureSegment(upper).closing.add(value);

		SortedMap<K, Segment<V>> subMap = segments.subMap(lower, upper);
		for (Map.Entry<K, Segment<V>> entry : subMap.entrySet()) {
			entry.getValue().set.add(value);
		}
	}

	@SuppressWarnings("ConstantConditions")
	public boolean remove(K lower, K upper, V value) {
		boolean removed = false;

		ensureSegment(lower);
		removed |= ensureSegment(upper).closing.remove(value);

		Map.Entry<K, Segment<V>> prevEntry = segments.lowerEntry(lower);
		Segment<V> prev = prevEntry != null ? prevEntry.getValue() : null;
		Iterator<Segment<V>> it = segments.subMap(lower, true, upper, true).values().iterator();
		while (it.hasNext()) {
			Segment<V> segment = it.next();
			removed |= segment.set.remove(value);
			if (segment.closing.isEmpty() && segment.set.equals(prev != null ? prev.set : emptySet())) {
				it.remove();
			} else {
				prev = segment;
			}
		}

		return removed;
	}

	public Set<V> get(K key) {
		LinkedHashSet<V> result = new LinkedHashSet<>();
		for (Map.Entry<K, Segment<V>> entry : segments.headMap(key, true).descendingMap().entrySet()) {
			result.addAll(entry.getValue().set);
			if (!entry.getKey().equals(key)) {
				break;
			}
			result.addAll(entry.getValue().closing);
		}
		return result;
	}

	public Set<V> getRange(K lower, K upper) {
		LinkedHashSet<V> result = new LinkedHashSet<>();

		Map.Entry<K, Segment<V>> floorEntry = segments.floorEntry(lower);
		if (floorEntry != null) {
			result.addAll(floorEntry.getValue().set);
		}

		SortedMap<K, Segment<V>> subMap = segments.subMap(lower, true, upper, true);
		for (Map.Entry<K, Segment<V>> entry : subMap.entrySet()) {
			result.addAll(entry.getValue().set);
			result.addAll(entry.getValue().closing);
		}

		return result;
	}

	public Set<V> getAll() {
		LinkedHashSet<V> result = new LinkedHashSet<>();
		for (Segment<V> segment : segments.values()) {
			result.addAll(segment.closing);
		}
		return result;
	}

}
