package io.activej.dataflow.calcite.utils;

import java.util.Comparator;

public final class NaturalNullsFirstComparator implements Comparator<Comparable<Object>> {
	private static final NaturalNullsFirstComparator INSTANCE = new NaturalNullsFirstComparator();

	public static <T extends Comparable<T>> Comparator<T> getInstance() {
		//noinspection unchecked
		return (Comparator<T>) INSTANCE;
	}

	@Override
	public int compare(Comparable<Object> o1, Comparable<Object> o2) {
		if (o1 == null) {
			return o2 == null ? 0 : -1;
		}
		if (o2 == null) {
			return 1;
		}
		return o1.compareTo(o2);
	}
}
