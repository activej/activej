package io.activej.dataflow.calcite.jdbc;

import io.activej.async.process.AsyncCloseable;
import io.activej.datastream.SynchronousStreamConsumer;
import io.activej.record.Record;
import org.apache.calcite.avatica.Meta.Frame;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class FrameFetcher implements AsyncCloseable {
	private final SynchronousStreamConsumer<Record> synchronousConsumer;
	private final int columnSize;

	private int taken;

	FrameFetcher(SynchronousStreamConsumer<Record> synchronousConsumer, int columnSize) {
		this.synchronousConsumer = synchronousConsumer;
		this.columnSize = columnSize;
	}

	public Frame fetch(long offset, int fetchMaxRowCount) {
		if (offset != taken) {
			throw new RuntimeException("Cannot return records from offset that is not equal to number of already taken records");
		}

		int fetchSize = fetchMaxRowCount == -1 ?
				synchronousConsumer.getBufferSize() :
				Math.min(fetchMaxRowCount, synchronousConsumer.getBufferSize());

		List<Object> rows = new ArrayList<>(fetchSize);

		for (int i = 0; i < fetchSize; i++) {
			Optional<Record> maybeRecord = synchronousConsumer.fetch();

			if (maybeRecord.isEmpty()) {
				break;
			}

			Object[] row = recordToRow(maybeRecord.get());
			rows.add(row);
		}

		taken += rows.size();

		return Frame.create(offset, synchronousConsumer.isDone(), rows);
	}

	private Object[] recordToRow(Record record) {
		Object[] row = new Object[columnSize];
		for (int i = 0; i < columnSize; i++) {
			row[i] = record.get(i);
		}
		return row;
	}

	@Override
	public void closeEx(@NotNull Exception e) {
		synchronousConsumer.closeEx(e);
	}
}
