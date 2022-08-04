package io.activej.dataflow.calcite.utils;

import java.util.Comparator;

public final class EqualObjectComparator<T> implements Comparator<T> {
	private static final EqualObjectComparator<Object> INSTANCE = new EqualObjectComparator<>();

	private EqualObjectComparator() {
	}

	public static <T> EqualObjectComparator<T> getInstance() {
		//noinspection unchecked
		return (EqualObjectComparator<T>) INSTANCE;
	}

	@Override
	public int compare(T item1, T item2) {
		return 0;
	}
}
