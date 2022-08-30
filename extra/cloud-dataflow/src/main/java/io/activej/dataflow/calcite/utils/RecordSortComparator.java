package io.activej.dataflow.calcite.utils;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeRecord;

import java.util.Comparator;
import java.util.List;

public final class RecordSortComparator implements Comparator<Record> {
	private final List<FieldSort> sorts;
	private final Comparator<Record> recordComparator;

	public RecordSortComparator(@Deserialize("sorts") List<FieldSort> sorts) {
		this.sorts = sorts;

		Comparator<Record> comparator = EqualObjectComparator.getInstance();

		for (FieldSort sort : sorts) {
			Comparator<Record> nextComparator = sort.toComparator();
			comparator = comparator.thenComparing(nextComparator);
		}

		this.recordComparator = comparator;
	}

	@Serialize(order = 1)
	public List<FieldSort> getSorts() {
		return sorts;
	}

	@Override
	public int compare(Record o1, Record o2) {
		return recordComparator.compare(o1, o2);
	}

	@SerializeRecord
	public record FieldSort(int index, boolean asc) {
		public <K extends Comparable<K>> Comparator<Record> toComparator() {
			return Comparator.<Record, K>comparing(record -> record.get(index), asc ? Comparator.naturalOrder() : Comparator.reverseOrder());
		}
	}
}