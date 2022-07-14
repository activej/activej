package io.activej.dataflow.calcite.jdbc;

import io.activej.datastream.AbstractStreamConsumer;
import io.activej.record.Record;
import org.apache.calcite.avatica.Meta.Frame;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

final class FrameConsumer extends AbstractStreamConsumer<Record> {
	private static final int DEFAULT_PREFETCH_COUNT = 100;

	private final int columnSize;

	private List<Record> records = new ArrayList<>(DEFAULT_PREFETCH_COUNT);
	private List<Record> newRecords = new ArrayList<>(DEFAULT_PREFETCH_COUNT);

	private @Nullable Exception error;

	private int prefetchCount = DEFAULT_PREFETCH_COUNT;
	private int taken;
	private boolean done;
	private boolean doneSent;

	FrameConsumer(int columnSize) {
		this.columnSize = columnSize;
	}

	public synchronized Frame fetch(long offset, int fetchMaxRowCount) {
		if (error != null) {
			throw new RuntimeException(error);
		}

		if (offset != taken) {
			System.out.println("Offset: " + offset + ", taken: " + taken);
		}
		if (done) {
			if (doneSent) {
				throw new AssertionError();
			}
			doneSent = true;
		}

		taken += records.size();
		prefetchCount = fetchMaxRowCount;

		List<Record> records = new ArrayList<>(this.records);
		this.records = newRecords;
		newRecords = new ArrayList<>(prefetchCount);

		List<Object> rows = new ArrayList<>(records.size());
		for (Record record : records) {
			rows.add(recordToRow(record));
		}

		return Frame.create(offset, done, rows);
	}

	@Override
	protected void onInit() {
		doResume();
	}

	@Override
	protected void onError(Exception e) {
		error = e;
	}

	private void doResume() {
		resume(item -> {
			synchronized (this) {
				if (records.size() > prefetchCount) {
					newRecords.add(item);
					suspend();
				} else {
					records.add(item);
				}
			}
		});
	}

	@Override
	protected void onEndOfStream() {
		done = true;
	}

	private Object[] recordToRow(Record record) {
		Object[] row = new Object[columnSize];
		for (int i = 0; i < columnSize; i++) {
			row[i] = record.get(i);
		}
		return row;
	}
}
