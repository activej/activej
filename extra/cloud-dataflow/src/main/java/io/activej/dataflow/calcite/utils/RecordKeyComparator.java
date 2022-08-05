package io.activej.dataflow.calcite.utils;

import io.activej.record.Record;

import java.util.Comparator;

public class RecordKeyComparator implements Comparator<Record> {
	private static final RecordKeyComparator INSTANCE = new RecordKeyComparator();

	private RecordKeyComparator() {
	}

	public static RecordKeyComparator getInstance() {
		return INSTANCE;
	}

	@Override
	public int compare(Record o1, Record o2) {
		assert o1.getScheme() == o2.getScheme();

		Comparator<Record> comparator = o1.getScheme().recordComparator();
		return comparator.compare(o1, o2);
	}

}
