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

import org.jetbrains.annotations.ApiStatus.Internal;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;

/**
 * Utility helper methods for internal use
 */
@Internal
public class Utils {
	public static final int TO_STRING_LIMIT = ApplicationSettings.getInt(Utils.class, "toStringLimit", 10);

	public static <T> T nonNullElse(@Nullable T value1, T defaultValue) {
		return value1 != null ? value1 : defaultValue;
	}

	public static <T> T nonNullElseGet(@Nullable T value, Supplier<? extends T> defaultValue) {
		return value != null ? value : defaultValue.get();
	}

	public static String nonNullElseEmpty(@Nullable String value) {
		return nonNullElse(value, "");
	}

	public static <T> Set<T> nonNullElseEmpty(@Nullable Set<T> set) {
		return nonNullElse(set, Set.of());
	}

	public static <T> List<T> nonNullElseEmpty(@Nullable List<T> list) {
		return nonNullElse(list, List.of());
	}

	public static <K, V> Map<K, V> nonNullElseEmpty(@Nullable Map<K, V> map) {
		return nonNullElse(map, Map.of());
	}

	public static <T, E extends Throwable> T nonNullOrException(@Nullable T value, Supplier<E> exceptionSupplier) throws E {
		if (value != null) {
			return value;
		}
		throw exceptionSupplier.get();
	}

	@Contract("_, _ -> null")
	public static <V> @Nullable V nullify(@Nullable V value, Consumer<? super V> action) {
		if (value != null) {
			action.accept(value);
		}
		return null;
	}

	@Contract("_, _, _ -> null")
	public static <V, A> @Nullable V nullify(@Nullable V value, BiConsumer<? super V, A> action, A actionArg) {
		if (value != null) {
			action.accept(value, actionArg);
		}
		return null;
	}

	public static <V> @Nullable V replace(@Nullable V value, @Nullable V newValue, Consumer<? super V> action) {
		if (value != null && value != newValue) {
			action.accept(value);
		}
		return newValue;
	}

	public static <V, A> @Nullable V replace(@Nullable V value, @Nullable V newValue, BiConsumer<? super V, A> action, A actionArg) {
		if (value != null && value != newValue) {
			action.accept(value, actionArg);
		}
		return newValue;
	}

	public static boolean arraysEquals(
		byte[] array1, int pos1, int len1,
		byte[] array2, int pos2, int len2
	) {
		if (len1 != len2) return false;
		for (int i = 0; i < len1; i++) {
			if (array1[pos1 + i] != array2[pos2 + i]) {
				return false;
			}
		}
		return true;
	}

	public static boolean isBijection(Map<?, ?> map) {
		return new HashSet<>(map.values()).size() == map.size();
	}

	public static <T> Stream<T> iterate(Supplier<? extends T> supplier, Predicate<? super T> hasNext) {
		return iterate(supplier.get(), hasNext, $ -> supplier.get());
	}

	public static <T> Stream<T> iterate(T seed, Predicate<? super T> hasNext, UnaryOperator<T> f) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
			new Iterator<>() {
				T item = seed;

				@Override
				public boolean hasNext() {
					return hasNext.test(item);
				}

				@Override
				public T next() {
					T next = item;
					item = f.apply(item);
					return next;
				}
			},
			Spliterator.ORDERED | Spliterator.IMMUTABLE), false);
	}

	public static <T> BinaryOperator<T> noMergeFunction() {
		return (v, v2) -> {
			throw new AssertionError();
		};
	}

	public static int initialCapacity(int initialSize) {
		return (initialSize + 2) / 3 * 4;
	}

	public static <T> String toString(Collection<? extends T> collection) {
		return toString(collection, TO_STRING_LIMIT);
	}

	public static <K, V> String toString(Map<? extends K, ? extends V> map) {
		return toString(map, TO_STRING_LIMIT);
	}

	public static <T> String toString(Collection<? extends T> collection, int limit) {
		return collection.stream()
			.limit(limit)
			.map(Object::toString)
			.collect(joining(",", "[", collection.size() <= limit ? "]" : ", ..and " + (collection.size() - limit) + " more]"));
	}

	public static <K, V> String toString(Map<? extends K, ? extends V> map, int limit) {
		return map.entrySet().stream()
			.limit(limit)
			.map(element -> element.getKey() + "=" + element.getValue())
			.collect(joining(",", "{",
				map.size() <= limit ?
					"}" :
					", ..and " + (map.size() - limit) + " more}"));
	}
}
