package io.activej.dataflow.calcite.jdbc;

import io.activej.datastream.AbstractStreamConsumer;
import io.activej.record.Record;
import org.apache.calcite.avatica.Meta.Frame;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class FrameConsumer extends AbstractStreamConsumer<Record> {
	private static final int DEFAULT_PREFETCH_SIZE = 100;

	private static final Record EOS_RECORD = new Record(null) {};

	private final int columnSize;

	private final ConcurrentLinkedQueue<Record> recordQueue = new ConcurrentLinkedQueue<>();

	private @Nullable Exception error;

	private final AtomicInteger prefetchSize = new AtomicInteger(DEFAULT_PREFETCH_SIZE);
	private final AtomicBoolean suspended = new AtomicBoolean(false);

	private int taken;
	private boolean done;
	private boolean doneSent;

	FrameConsumer(int columnSize) {
		this.columnSize = columnSize;
	}

	public Frame fetch(long offset, int fetchMaxRowCount) {
		if (error != null) {
			throw new RuntimeException(error);
		}

		if (offset != taken) {
			throw new RuntimeException("Cannot return less records than already taken");
		}
		if (done) {
			if (doneSent) {
				throw new AssertionError();
			}
			doneSent = true;
		}

		if (fetchMaxRowCount != -1) {
			prefetchSize.set(fetchMaxRowCount);
		}

		List<Object> rows = new ArrayList<>();

		while (fetchMaxRowCount != 0) {
			Record record = recordQueue.poll();
			if (record == null) {
				if (suspended.compareAndSet(true, false)) {
					eventloop.submit(this::doResume);
				}
				break;
			}
			if (record == EOS_RECORD) {
				done = true;
				break;
			}

			Object[] row = recordToRow(record);
			rows.add(row);

			fetchMaxRowCount--;
		}

		taken += rows.size();

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
			recordQueue.add(item);

			if (recordQueue.size() >= prefetchSize.get()) {
				suspend();
				suspended.set(true);
			}
		});
	}

	@Override
	protected void onEndOfStream() {
		recordQueue.add(EOS_RECORD);
		acknowledge();
	}

	private Object[] recordToRow(Record record) {
		Object[] row = new Object[columnSize];
		for (int i = 0; i < columnSize; i++) {
			row[i] = record.get(i);
		}
		return row;
	}
}
