package io.activej.dataflow.calcite.utils;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeRecord;

import java.util.Comparator;
import java.util.List;

import static io.activej.common.Checks.checkArgument;

public final class RecordSortComparator implements Comparator<Record> {
	private final List<FieldSort> sorts;
	private final Comparator<Record> recordComparator;

	public RecordSortComparator(@Deserialize("sorts") List<FieldSort> sorts) {
		checkArgument(sorts.size() != 0);

		this.sorts = sorts;

		FieldSort first = sorts.get(0);
		Comparator<Record> comparator = first.toComparator();

		for (int i = 1; i < sorts.size(); i++) {
			Comparator<Record> nextComparator = sorts.get(i).toComparator();
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
