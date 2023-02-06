package io.activej.datastream.processor.transformer.sort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Function;

public final class DistinctIterator<K, T> implements Iterator<T> {
	private final ArrayList<T> sortedList;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	int i = 0;

	DistinctIterator(ArrayList<T> sortedList, Function<T, K> keyFunction, Comparator<K> keyComparator) {
		this.sortedList = sortedList;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
	}

	@Override
	public boolean hasNext() {
		return i < sortedList.size();
	}

	@Override
	public T next() {
		T next = sortedList.get(i++);
		K nextKey = keyFunction.apply(next);
		while (i < sortedList.size()) {
			if (keyComparator.compare(nextKey, keyFunction.apply(sortedList.get(i))) == 0) {
				i++;
				continue;
			}
			break;
		}
		return next;
	}
}
