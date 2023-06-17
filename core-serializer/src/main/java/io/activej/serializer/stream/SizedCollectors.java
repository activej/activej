package io.activej.serializer.stream;

import java.util.*;

import static io.activej.serializer.stream.SizedCollectorsKV.hashInitialSize;
import static java.util.Collections.*;

class SizedCollectors {
	public static <T> SizedCollector<T, T[], Collection<T>> toCollection() {
		//noinspection unchecked,rawtypes
		return (SizedCollector) SizedCollectors.toList();
	}

	public static <T> SizedCollector<T, T[], List<T>> toList() {
		return new SizedCollector<>() {
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

	public static <T> SizedCollector<T, ArrayList<T>, ArrayList<T>> toArrayList() {
		return SizedCollector.toCollection(ArrayList::new);
	}

	public static <T> SizedCollector<T, LinkedList<T>, LinkedList<T>> toLinkedList() {
		return SizedCollector.toCollection($ -> new LinkedList<>());
	}

	public static <T> SizedCollector<T, HashSet<T>, Set<T>> toSet() {
		return new SizedCollector<>() {
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
				return new HashSet<>(hashInitialSize(size));
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

	public static <T> SizedCollector<T, HashSet<T>, HashSet<T>> toHashSet() {
		return SizedCollector.toCollection(size -> new HashSet<>(hashInitialSize(size)));
	}

	public static <T> SizedCollector<T, LinkedHashSet<T>, LinkedHashSet<T>> toLinkedHashSet() {
		return SizedCollector.toCollection(size -> new LinkedHashSet<>(hashInitialSize(size)));
	}

	public static <T extends Enum<T>> SizedCollector<T, EnumSet<T>, EnumSet<T>> toEnumSet(Class<T> type) {
		return SizedCollector.toCollection(size -> EnumSet.noneOf(type));
	}

}
