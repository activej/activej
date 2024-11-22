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

import java.util.*;

import static io.activej.common.Utils.initialCapacity;
import static java.lang.Math.max;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;

public final class CollectionUtils {
	/**
	 * Concatenates two lists into one
	 * <p>
	 * No guarantee on a mutability of a resulting list is made,
	 * so list should be considered unmodifiable
	 * <p>
	 * If any of concatenated lists is modified, the behaviour of a concatenated list is undefined
	 *
	 * @throws NullPointerException may be thrown if any list is {@code null} or contains {@code null} elements
	 */
	@SuppressWarnings("unchecked")
	public static <D> List<D> concat(List<? extends D> list1, List<? extends D> list2) {
		if (list1.isEmpty()) return (List<D>) list2;
		if (list2.isEmpty()) return (List<D>) list1;
		Object[] objects = new Object[list1.size() + list2.size()];
		System.arraycopy(list1.toArray(), 0, objects, 0, list1.size());
		System.arraycopy(list2.toArray(), 0, objects, list1.size(), list2.size());
		return (List<D>) List.of(objects);
	}

	/**
	 * Returns a difference of two sets
	 * <p>
	 * No guarantee on a mutability of a resulting set is made,
	 * so set should be considered unmodifiable
	 * <p>
	 * If any of provided sets is modified, the behaviour of a resulting set is undefined
	 */
	@SuppressWarnings("unchecked")
	public static <T> Set<T> difference(Set<? extends T> a, Set<? extends T> b) {
		if (b.isEmpty()) return (Set<T>) a;
		return a.stream().filter(not(b::contains)).collect(toSet());
	}

	/**
	 * Returns an intersection of two sets
	 * <p>
	 * No guarantee on a mutability of a resulting set is made,
	 * so set should be considered unmodifiable
	 * <p>
	 * If any of provided sets is modified, the behaviour of a resulting set is undefined
	 */
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

	/**
	 * Returns a union of two sets
	 * <p>
	 * No guarantee on a mutability of a resulting set is made,
	 * so set should be considered unmodifiable
	 * <p>
	 * If any of provided sets is modified, the behaviour of a resulting set is undefined
	 */
	@SuppressWarnings("unchecked")
	public static <T> Set<T> union(Set<? extends T> a, Set<? extends T> b) {
		if (a.isEmpty()) return (Set<T>) b;
		if (b.isEmpty()) return (Set<T>) a;
		Set<T> result = new HashSet<>(max(a.size(), b.size()));
		result.addAll(a);
		result.addAll(b);
		return result;
	}

	public static <T> T first(List<? extends T> list) {
		return list.get(0);
	}

	public static <T> T first(Iterable<? extends T> iterable) {
		return iterable.iterator().next();
	}

	public static <T> T last(List<? extends T> list) {
		return list.get(list.size() - 1);
	}

	public static <T> T last(Iterable<? extends T> iterable) {
		Iterator<? extends T> iterator = iterable.iterator();
		while (true) {
			T value = iterator.next();
			if (!iterator.hasNext()) return value;
		}
	}

	public static <K, V> HashMap<K, V> newHashMap(int initialSize) {
		return new HashMap<>(initialCapacity(initialSize));
	}

	public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int initialSize) {
		return new LinkedHashMap<>(initialCapacity(initialSize));
	}

	public static <T> HashSet<T> newHashSet(int initialSize) {
		return new HashSet<>(initialCapacity(initialSize));
	}

	public static <T> LinkedHashSet<T> newLinkedHashSet(int initialSize) {
		return new LinkedHashSet<>(initialCapacity(initialSize));
	}
}
