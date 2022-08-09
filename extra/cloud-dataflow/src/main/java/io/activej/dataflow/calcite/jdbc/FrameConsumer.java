package io.activej.dataflow.calcite.jdbc;

import io.activej.common.ApplicationSettings;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.record.Record;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.Frame;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class FrameConsumer extends AbstractStreamConsumer<Record> {
	private static final int FETCH_SIZE = ApplicationSettings.getInt(FrameConsumer.class, "fetchSize", AvaticaStatement.DEFAULT_FETCH_SIZE);

	private static final Record FINISH_RECORD = new Record(null) {};

	private final int columnSize;

	private final LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<>();

	private @Nullable Exception error;

	private final AtomicBoolean suspended = new AtomicBoolean(false);

	private int taken;
	private boolean done;

	FrameConsumer(int columnSize) {
		this.columnSize = columnSize;
	}

	public Frame fetch(long offset, int fetchMaxRowCount) {
		if (error != null) {
			throw new RuntimeException(error);
		}

		if (offset != taken) {
			throw new RuntimeException("Cannot return records from offset that is not equal to number of already taken records");
		}
		if (done) {
			throw new AssertionError();
		}

		int fetchSize = Math.min(Math.max(0, fetchMaxRowCount), FETCH_SIZE);

		List<Object> rows = new ArrayList<>(fetchSize);

		for (int i = 0; i < fetchSize; i++) {
			Record record;
			try {
				record = recordQueue.take();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}

			if (record == FINISH_RECORD) {
				if (error == null) {
					done = true;
					eventloop.submit(this::acknowledge);
				} else if (i == 0) {
					throw new RuntimeException(error);
				}
				break;
			}
			if (suspended.compareAndSet(true, false)) {
				eventloop.submit(this::doResume);
			}

			Object[] row = recordToRow(record);
			rows.add(row);
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
		recordQueue.add(FINISH_RECORD);
	}

	private void doResume() {
		resume(item -> {
			if (recordQueue.size() > FETCH_SIZE) {
				suspend();
				suspended.set(true);
			}
			recordQueue.add(item);
		});
	}

	@Override
	protected void onEndOfStream() {
		recordQueue.add(FINISH_RECORD);
	}

	private Object[] recordToRow(Record record) {
		Object[] row = new Object[columnSize];
		for (int i = 0; i < columnSize; i++) {
			row[i] = record.get(i);
		}
		return row;
	}
}
