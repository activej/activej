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

import io.activej.common.time.CurrentTimeProvider;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.util.stream.Collectors.toMap;

public final class LWWSet<E> implements Set<E>, CrdtType<LWWSet<E>> {
	private final Map<E, Timestamps> set;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private LWWSet(Map<E, Timestamps> set) {
		this.set = set;
	}

	public LWWSet() {
		this(new HashMap<>());
	}

	@SafeVarargs
	public static <T> LWWSet<T> of(T... items) {
		LWWSet<T> set = new LWWSet<>();
		set.addAll(Arrays.asList(items));
		return set;
	}

	@Override
	public LWWSet<E> merge(LWWSet<E> other) {
		Map<E, Timestamps> newSet = new HashMap<>(set);
		for (Entry<E, Timestamps> entry : other.set.entrySet()) {
			newSet.merge(entry.getKey(), entry.getValue(), (ts1, ts2) -> new Timestamps(max(ts1.added, ts2.added), max(ts1.removed, ts2.removed)));
		}
		return new LWWSet<>(newSet);
	}

	@Override
	public @Nullable LWWSet<E> extract(long timestamp) {
		Map<E, Timestamps> newSet = set.entrySet().stream()
				.filter(entry -> entry.getValue().added > timestamp || entry.getValue().removed > timestamp)
				.collect(toMap(Entry::getKey, Entry::getValue));
		if (newSet.isEmpty()) {
			return null;
		}
		return new LWWSet<>(newSet);
	}

	@Override
	public Stream<E> stream() {
		return set.entrySet().stream().filter(e -> e.getValue().exists()).map(Entry::getKey);
	}

	@Override
	public int size() {
		//noinspection ReplaceInefficientStreamCount ikwiad
		return (int) stream().count();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(Object o) {
		//noinspection SuspiciousMethodCalls
		Timestamps timestamps = set.get(o);
		return timestamps != null && timestamps.added >= timestamps.removed;
	}

	@Override
	public @NotNull Iterator<E> iterator() {
		return stream().iterator();
	}

	@Override
	public Object @NotNull [] toArray() {
		//noinspection SimplifyStreamApiCallChains
		return stream().toArray();
	}

	@SuppressWarnings("SuspiciousToArrayCall")
	@Override
	public <T> T @NotNull [] toArray(T @NotNull [] a) {
		return stream().toArray($ -> a);
	}

	@Override
	public boolean add(E t) {
		Timestamps timestamps = set.get(t);
		if (timestamps == null) {
			set.put(t, new Timestamps(now.currentTimeMillis(), 0));
			return true;
		}
		boolean notExisted = !timestamps.exists();
		timestamps.added = now.currentTimeMillis();
		return notExisted;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean remove(Object o) {
		//noinspection SuspiciousMethodCalls
		Timestamps timestamps = set.get(o);
		if (timestamps == null) {
			set.put((E) o, new Timestamps(0, now.currentTimeMillis()));
			return false;
		}
		boolean existed = timestamps.exists();
		timestamps.removed = now.currentTimeMillis();
		return existed;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return stream().allMatch(c::contains);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		boolean added = false;
		for (E e : c) {
			added |= add(e);
		}
		return added;
	}

	@Override
	public boolean retainAll(@NotNull Collection<?> c) {
		boolean removed = false;
		for (E item : this) {
			if (!c.contains(item)) {
				remove(item);
				removed = true;
			}
		}
		return removed;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean removed = false;
		for (Object o : c) {
			removed |= remove(o);
		}
		return removed;
	}

	@Override
	public void clear() {
		forEach(this::remove);
	}

	@Override
	public String toString() {
		return set.entrySet()
				.stream()
				.filter(e -> e.getValue().exists())
				.map(e -> Objects.toString(e.getKey()))
				.collect(Collectors.joining(", ", "[", "]"));
	}

	private static final class Timestamps {
		long added, removed;

		Timestamps(long added, long removed) {
			this.added = added;
			this.removed = removed;
		}

		boolean exists() {
			return added >= removed;
		}
	}

	public static class Serializer<T> implements BinarySerializer<LWWSet<T>> {
		private final BinarySerializer<T> valueSerializer;

		public Serializer(BinarySerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, LWWSet<T> item) {
			out.writeVarInt(item.set.size());
			for (Entry<T, Timestamps> entry : item.set.entrySet()) {
				valueSerializer.encode(out, entry.getKey());
				Timestamps timestamps = entry.getValue();
				out.writeLong(timestamps.added);
				out.writeLong(timestamps.removed);
			}
		}

		@Override
		public LWWSet<T> decode(BinaryInput in) {
			int size = in.readVarInt();
			Map<T, Timestamps> set = new HashMap<>(size);
			for (int i = 0; i < size; i++) {
				set.put(valueSerializer.decode(in), new Timestamps(in.readLong(), in.readLong()));
			}
			return new LWWSet<>(set);
		}
	}
}
