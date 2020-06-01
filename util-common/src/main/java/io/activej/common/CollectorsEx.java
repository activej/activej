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

package io.activej.common;

import io.activej.common.collection.CollectionUtils;
import io.activej.common.ref.Ref;
import io.activej.common.ref.RefBoolean;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.activej.common.collection.CollectionUtils.set;

public class CollectorsEx {

	public static final Collector<Object, Void, Void> TO_VOID = Collector.of(() -> null, (a, v) -> {}, (a1, a2) -> null, a -> null);

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, Void, Void> toVoid() {
		return (Collector<T, Void, Void>) TO_VOID;
	}

	public static <T> Collector<T, Ref<T>, T> toFirst() {
		return Collector.of(
				Ref::new,
				(a, v) -> { if (a.value == null) a.value = v; },
				(a1, a2) -> a1,
				a -> a.value);
	}

	public static <T> Collector<T, Ref<T>, T> toLast() {
		return Collector.of(
				Ref::new,
				(a, v) -> a.value = v,
				(a1, a2) -> a1,
				a -> a.value);
	}

	private static final Collector<Boolean, RefBoolean, Boolean> TO_ALL =
			Collector.of(
					() -> new RefBoolean(true),
					(a, t) -> a.value &= t,
					(a1, a2) -> {
						a1.value &= a2.value;
						return a1;
					},
					b -> b.value);

	private static final Collector<Boolean, RefBoolean, Boolean> TO_ANY =
			Collector.of(
					() -> new RefBoolean(false),
					(a, t) -> a.value |= t,
					(a1, a2) -> {
						a1.value |= a2.value;
						return a1;
					},
					b -> b.value);

	private static final Collector<Boolean, RefBoolean, Boolean> TO_NONE =
			Collector.of(
					() -> new RefBoolean(true),
					(a, t) -> a.value &= !t,
					(a1, a2) -> {
						a1.value &= a2.value;
						return a1;
					},
					b -> b.value);

	public static <T> Collector<T, RefBoolean, Boolean> toAll(Predicate<? super T> predicate) {
		return Collector.of(
				() -> new RefBoolean(true),
				(a, t) -> a.value &= predicate.test(t),
				(a1, a2) -> {
					a1.value &= a2.value;
					return a1;
				},
				b -> b.value);
	}

	public static Collector<Boolean, RefBoolean, Boolean> toAll() {
		return TO_ALL;
	}

	public static <T> Collector<T, RefBoolean, Boolean> toAny(Predicate<T> predicate) {
		return Collector.of(
				() -> new RefBoolean(false),
				(a, t) -> a.value |= predicate.test(t),
				(a1, a2) -> {
					a1.value |= a2.value;
					return a1;
				},
				b -> b.value);
	}

	public static Collector<Boolean, RefBoolean, Boolean> toAny() {
		return TO_ANY;
	}

	public static <T> Collector<T, RefBoolean, Boolean> toNone(Predicate<T> predicate) {
		return Collector.of(
				() -> new RefBoolean(true),
				(a, t) -> a.value &= !predicate.test(t),
				(a1, a2) -> {
					a1.value &= a2.value;
					return a1;
				},
				b -> b.value);
	}

	public static Collector<Boolean, RefBoolean, Boolean> toNone() {
		return TO_NONE;
	}

	public static <T> BinaryOperator<T> throwingMerger() {
		return (u, v) -> { throw new IllegalStateException("Duplicate key " + u); };
	}

	public static <K, V> Collector<Entry<K, V>, ?, Map<K, V>> toMap() {
		return Collectors.toMap(Entry::getKey, Entry::getValue);
	}

	public static <T, K, V> Collector<T, ?, Map<K, Set<V>>> toMultimap(Function<? super T, ? extends K> keyMapper,
			Function<? super T, ? extends V> valueMapper) {
		return Collectors.toMap(keyMapper, t -> set(valueMapper.apply(t)), CollectionUtils::union);
	}
}
