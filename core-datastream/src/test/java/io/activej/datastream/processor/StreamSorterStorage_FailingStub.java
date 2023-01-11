package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;

import java.util.List;

import static io.activej.common.Checks.checkNotNull;

public final class StreamSorterStorage_FailingStub<T> implements AsyncStreamSorterStorage<T> {
	static final Exception STORAGE_EXCEPTION = new ExpectedException("failing storage");

	private AsyncStreamSorterStorage<T> storage;

	boolean failNewPartition;
	boolean failWrite;
	boolean failRead;
	boolean failCleanup;

	private StreamSorterStorage_FailingStub(AsyncStreamSorterStorage<T> storage) {
		this.storage = storage;
	}

	public static <T> StreamSorterStorage_FailingStub<T> create(AsyncStreamSorterStorage<T> storage) {
		return new StreamSorterStorage_FailingStub<>(storage);
	}

	public static <T> StreamSorterStorage_FailingStub<T> create() {
		return new StreamSorterStorage_FailingStub<>(null);
	}

	public StreamSorterStorage_FailingStub<T> withFailNewPartition() {
		this.failNewPartition = true;
		return this;
	}

	public StreamSorterStorage_FailingStub<T> withFailWrite() {
		this.failWrite = true;
		return this;
	}

	public StreamSorterStorage_FailingStub<T> withFailRead() {
		this.failRead = true;
		return this;
	}

	public StreamSorterStorage_FailingStub<T> withFailCleanup() {
		this.failCleanup = true;
		return this;
	}

	public void setStorage(AsyncStreamSorterStorage<T> storage) {
		this.storage = storage;
	}

	@Override
	public Promise<Integer> newPartitionId() {
		checkNotNull(storage);
		return failNewPartition ? Promise.ofException(STORAGE_EXCEPTION) : storage.newPartitionId();
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		checkNotNull(storage);
		return failWrite ? Promise.ofException(STORAGE_EXCEPTION) : storage.write(partition);
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		checkNotNull(storage);
		return failRead ? Promise.ofException(STORAGE_EXCEPTION) : storage.read(partition);
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		checkNotNull(storage);
		return failCleanup ? Promise.ofException(STORAGE_EXCEPTION) : storage.cleanup(partitionsToDelete);
	}
}
