package io.activej.dataflow.calcite.jdbc;

import io.activej.async.process.AsyncCloseable;
import io.activej.datastream.consumer.BlockingStreamConsumer;
import io.activej.record.Record;
import org.apache.calcite.avatica.Meta.Frame;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.activej.datastream.processor.transformer.impl.Limiter.NO_LIMIT;

public final class FrameFetcher implements AsyncCloseable {
	private final BlockingStreamConsumer<Record> blockingConsumer;
	private final int columnSize;

	private long taken;
	private long limit;

	FrameFetcher(BlockingStreamConsumer<Record> blockingConsumer, int columnSize, long limit) {
		this.blockingConsumer = blockingConsumer;
		this.columnSize = columnSize;
		this.limit = limit;
	}

	public Frame fetch(long offset, int fetchMaxRowCount) {
		if (offset != taken) {
			throw new RuntimeException("Cannot return records from offset that is not equal to number of already taken records");
		}

		int fetchSize = fetchMaxRowCount == -1 ?
			blockingConsumer.getBufferCapacity() :
			Math.min(fetchMaxRowCount, blockingConsumer.getBufferCapacity());

		List<Object> rows = new ArrayList<>(fetchSize);

		boolean done = false;
		for (int i = 0; i < fetchSize; i++) {
			@Nullable Record maybeRecord;
			try {
				maybeRecord = blockingConsumer.take();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw new RuntimeException(e.getCause());
			}

			if (maybeRecord != null) {
				Object[] row = recordToRow(maybeRecord);
				rows.add(row);
				if (limit == NO_LIMIT || --limit != 0) {
					continue;
				}
			}

			done = true;
			try {
				blockingConsumer.submitAcknowledgement().get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
			break;

		}

		taken += rows.size();

		return Frame.create(offset, done, rows);
	}

	private Object[] recordToRow(Record record) {
		Object[] row = new Object[columnSize];
		for (int i = 0; i < columnSize; i++) {
			row[i] = record.get(i);
		}
		return row;
	}

	@Override
	public void closeEx(Exception e) {
		blockingConsumer.closeEx(e);
	}
}
