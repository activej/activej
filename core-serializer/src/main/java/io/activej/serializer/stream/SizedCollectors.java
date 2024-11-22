package io.activej.serializer.stream;

import io.activej.common.collection.CollectionUtils;

import java.util.*;
import java.util.function.IntFunction;

import static io.activej.common.collection.CollectionUtils.newHashSet;
import static java.util.Collections.*;

public class SizedCollectors {

	public static <T, C extends Collection<T>> SizedCollector<T, ?, C> toCollection(IntFunction<? extends C> factory) {
		return new SizedCollector<T, C, C>() {
			@Override
			public C accumulator(int size) {
				return factory.apply(size);
			}

			@Override
			public void accumulate(C accumulator, int index, T item) {
				accumulator.add(item);
			}

			@Override
			public C result(C accumulator) {
				return accumulator;
			}
		};
	}

	public static <T> SizedCollector<T, ?, Collection<T>> toCollection() {
		//noinspection unchecked,rawtypes
		return (SizedCollector) SizedCollectors.toList();
	}

	public static <T> SizedCollector<T, ?, List<T>> toList() {
		return new SizedCollector<T, T[], List<T>>() {
			@Override
			public List<T> create0() {
				return emptyList();
			}

			@Override
			public List<T> create1(T item) {
				return singletonList(item);
			}

			@Override
			public T[] accumulator(int size) {
				//noinspection unchecked
				return (T[]) new Object[size];
			}

			@Override
			public void accumulate(T[] accumulator, int index, T item) {
				accumulator[index] = item;
			}

			@Override
			public List<T> result(T[] accumulator) {
				return Arrays.asList(accumulator);
			}
		};
	}

	public static <T> SizedCollector<T, ?, ArrayList<T>> toArrayList() {
		return toCollection(ArrayList::new);
	}

	public static <T> SizedCollector<T, ?, LinkedList<T>> toLinkedList() {
		return toCollection($ -> new LinkedList<>());
	}

	public static <T> SizedCollector<T, ?, Set<T>> toSet() {
		return new SizedCollector<T, HashSet<T>, Set<T>>() {
			@Override
			public Set<T> create0() {
				return emptySet();
			}

			@Override
			public Set<T> create1(T item) {
				return singleton(item);
			}

			@Override
			public HashSet<T> accumulator(int size) {
				return newHashSet(size);
			}

			@Override
			public void accumulate(HashSet<T> accumulator, int index, T item) {
				accumulator.add(item);
			}

			@Override
			public Set<T> result(HashSet<T> accumulator) {
				return accumulator;
			}
		};
	}

	public static <T> SizedCollector<T, ?, HashSet<T>> toHashSet() {
		return toCollection(CollectionUtils::newHashSet);
	}

	public static <T> SizedCollector<T, ?, LinkedHashSet<T>> toLinkedHashSet() {
		return toCollection(CollectionUtils::newLinkedHashSet);
	}

	public static <T extends Enum<T>> SizedCollector<T, ?, EnumSet<T>> toEnumSet(Class<T> type) {
		return toCollection(size -> EnumSet.noneOf(type));
	}
}
