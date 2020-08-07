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

package io.activej.common.collection;

import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class CollectionUtils {

	public static <D> List<D> concat(Collection<? extends D> list1, Collection<? extends D> list2) {
		List<D> result = new ArrayList<>(list1.size() + list2.size());
		result.addAll(list1);
		result.addAll(list2);
		return result;
	}

	@SafeVarargs
	public static <T> Set<T> set(T... items) {
		return new LinkedHashSet<>(asList(items));
	}

	public static <T> Set<T> difference(Set<? extends T> a, Set<? extends T> b) {
		return a.stream().filter(t -> !b.contains(t)).collect(toSet());
	}

	public static <T> Set<T> intersection(Set<? extends T> a, Set<? extends T> b) {
		return a.size() < b.size() ?
				a.stream().filter(b::contains).collect(toSet()) :
				b.stream().filter(a::contains).collect(toSet());
	}

	public static <T> boolean hasIntersection(Set<? extends T> a, Set<? extends T> b) {
		return a.size() < b.size() ?
				a.stream().anyMatch(b::contains) :
				b.stream().anyMatch(a::contains);
	}

	public static <T> Set<T> union(Set<? extends T> a, Set<? extends T> b) {
		return Stream.concat(a.stream(), b.stream()).collect(toSet());
	}

	public static <T> Set<T> union(List<Set<? extends T>> sets) {
		return sets.stream().flatMap(Collection::stream).collect(toSet());
	}

	@SafeVarargs
	public static <T> Set<T> union(Set<? extends T>... sets) {
		return union(asList(sets));
	}

	public static <T> T first(Iterable<T> iterable) {
		return iterable.iterator().next();
	}

	public static <T> List<T> list() {
		return emptyList();
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> list(T... items) {
		return asList(items);
	}

	public static <T> Stream<T> iterate(Supplier<T> supplier, Predicate<T> hasNext) {
		return iterate(supplier.get(), hasNext, $ -> supplier.get());
	}

	public static <T> Stream<T> iterate(T seed, Predicate<T> hasNext, UnaryOperator<T> f) {
		requireNonNull(f);
		return iterate(
				new Iterator<T>() {
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
				});
	}

	public static <T> Stream<T> iterate(Iterator<T> iterator) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
				Spliterator.ORDERED | Spliterator.IMMUTABLE), false);
	}

	public static <T> String toLimitedString(Collection<T> collection, int limit) {
		return collection.stream()
				.limit(limit)
				.map(element -> element == collection ? "(this Collection)" : element.toString())
				.collect(joining(",", "[", collection.size() <= limit ? "]" : ", ..and " + (collection.size() - limit) + " more]"));
	}

	public static <K, V> String toLimitedString(Map<K, V> map, int limit) {
		return map.entrySet().stream()
				.limit(limit)
				.map(element -> {
					K key = element.getKey();
					V value = element.getValue();
					String keyString = key == map ? "(this Map)" : key.toString();
					String valueString = value == map ? "(this Map)" : value.toString();
					return keyString + '=' + valueString;
				})
				.collect(joining(",", "{", map.size() <= limit ? "}" : ", ..and " + (map.size() - limit) + " more}"));
	}

	private static final Iterator<Object> EMPTY_ITERATOR = new Iterator<Object>() {
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Object next() {
			throw new NoSuchElementException();
		}
	};

	@SuppressWarnings("unchecked")
	public static <T> Iterator<T> emptyIterator() {
		return (Iterator<T>) EMPTY_ITERATOR;
	}

	public static <T> Iterator<T> asIterator(T item) {
		return new Iterator<T>() {
			boolean hasNext = true;

			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				hasNext = false;
				return item;
			}
		};
	}

	public static <T> Iterator<T> asIterator(T item1, T item2) {
		return new Iterator<T>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < 2;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return i++ == 0 ? item1 : item2;
			}
		};
	}

	@SafeVarargs
	public static <T> Iterator<T> asIterator(T... items) {
		return new Iterator<T>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < items.length;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return items[i++];
			}
		};
	}

	@SafeVarargs
	public static <T> Iterator<T> asIterator(T head, T... tail) {
		return new Iterator<T>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i <= tail.length;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return i == 0 ? head : tail[i++ - 1];
			}
		};
	}

	public static <T, R> Iterator<R> transformIterator(Iterator<T> iterator, Function<T, R> fn) {
		return new Iterator<R>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public R next() {
				return fn.apply(iterator.next());
			}
		};
	}

	public static <T> Iterator<T> concat(Iterator<? extends T> iterator1, Iterator<? extends T> iterator2) {
		return concatIterators(asIterator(iterator1, iterator2));
	}

	public static <T> Iterator<T> append(Iterator<T> iterator, T value) {
		return concatIterators(asIterator(iterator, asIterator(value)));
	}

	public static <T> Iterator<T> prepend(T value, Iterator<T> iterator) {
		return concatIterators(asIterator(asIterator(value), iterator));
	}

	public static <T> Iterator<T> concatIterators(Iterator<? extends Iterator<? extends T>> iterators) {
		return new Iterator<T>() {
			@Nullable Iterator<? extends T> it = iterators.hasNext() ? iterators.next() : null;

			@Override
			public boolean hasNext() {
				return it != null;
			}

			@Override
			public T next() {
				if (it == null) throw new NoSuchElementException();
				T next = it.next();
				if (!it.hasNext()) {
					it = iterators.hasNext() ? iterators.next() : null;
				}
				return next;
			}
		};
	}

	public static <K, V> Map<K, V> keysToMap(Set<K> keys, Function<K, V> fn) {
		return keysToMap(keys.stream(), fn);
	}

	public static <K, V> Map<K, V> keysToMap(Stream<K> stream, Function<K, V> fn) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		stream.forEach(key -> result.put(key, fn.apply(key)));
		return result;
	}

	public static <K, V> Map<K, V> entriesToMap(Stream<Map.Entry<K, V>> stream) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		stream.forEach(entry -> result.put(entry.getKey(), entry.getValue()));
		return result;
	}

	public static <K, V, T> Map<K, V> transformMapValues(Map<K, T> map, Function<T, V> function) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>(map.size());
		map.forEach((key, value) -> result.put(key, function.apply(value)));
		return result;
	}

	public static <K, V> Map<K, V> map() {
		return new LinkedHashMap<>();
	}

	public static <K, V> Map<K, V> map(K key1, V value1) {
		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		return map;
	}

	public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2) {
		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);
		return map;
	}

	public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3) {
		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);
		map.put(key3, value3);
		return map;
	}

	public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);
		map.put(key3, value3);
		map.put(key4, value4);
		return map;
	}

	public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5) {
		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);
		map.put(key3, value3);
		map.put(key4, value4);
		map.put(key5, value5);
		return map;
	}

	public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5, K key6, V value6) {
		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);
		map.put(key3, value3);
		map.put(key4, value4);
		map.put(key5, value5);
		map.put(key6, value6);
		return map;
	}

	public static <T> T getLast(Iterable<T> iterable) {
		Iterator<T> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			T next = iterator.next();
			if (!iterator.hasNext()) {
				return next;
			}
		}
		throw new IllegalArgumentException("Empty iterable");
	}

	public static <T> T getFirst(Iterable<T> iterable) {
		Iterator<T> iterator = iterable.iterator();
		if (iterator.hasNext()) {
			return iterator.next();
		}
		throw new IllegalArgumentException("Empty iterable");
	}

	public static boolean isBijection(Map<?, ?> map) {
		return new HashSet<>(map.values()).size() == map.size();
	}
}
