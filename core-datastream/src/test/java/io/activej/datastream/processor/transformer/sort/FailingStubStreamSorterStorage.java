package io.activej.datastream.processor.transformer.sort;

import io.activej.common.builder.AbstractBuilder;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;
import io.activej.test.ExpectedException;

import java.util.List;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class FailingStubStreamSorterStorage<T> extends ImplicitlyReactive
	implements IStreamSorterStorage<T> {
	static final Exception STORAGE_EXCEPTION = new ExpectedException("failing storage");

	private IStreamSorterStorage<T> storage;

	boolean failNewPartition;
	boolean failWrite;
	boolean failRead;
	boolean failCleanup;

	private FailingStubStreamSorterStorage() {
	}

	public static <T> FailingStubStreamSorterStorage<T>.Builder builder() {
		return new FailingStubStreamSorterStorage<T>().new Builder();
	}

	public static <T> FailingStubStreamSorterStorage<T> create() {
		return FailingStubStreamSorterStorage.<T>builder().build();
	}

	public final class Builder extends AbstractBuilder<Builder, FailingStubStreamSorterStorage<T>> {
		private Builder() {}

		public Builder withFailNewPartition() {
			checkNotBuilt(this);
			FailingStubStreamSorterStorage.this.failNewPartition = true;
			return this;
		}

		public Builder withFailWrite() {
			checkNotBuilt(this);
			FailingStubStreamSorterStorage.this.failWrite = true;
			return this;
		}

		public Builder withFailRead() {
			checkNotBuilt(this);
			FailingStubStreamSorterStorage.this.failRead = true;
			return this;
		}

		public Builder withFailCleanup() {
			checkNotBuilt(this);
			FailingStubStreamSorterStorage.this.failCleanup = true;
			return this;
		}

		@Override
		protected FailingStubStreamSorterStorage<T> doBuild() {
			return FailingStubStreamSorterStorage.this;
		}
	}

	public void setStorage(IStreamSorterStorage<T> storage) {
		this.storage = storage;
	}

	@Override
	public Promise<Integer> newPartitionId() {
		checkInReactorThread(this);
		checkNotNull(storage);
		return failNewPartition ? Promise.ofException(STORAGE_EXCEPTION) : storage.newPartitionId();
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		checkInReactorThread(this);
		checkNotNull(storage);
		return failWrite ? Promise.ofException(STORAGE_EXCEPTION) : storage.write(partition);
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		checkInReactorThread(this);
		checkNotNull(storage);
		return failRead ? Promise.ofException(STORAGE_EXCEPTION) : storage.read(partition);
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		checkInReactorThread(this);
		checkNotNull(storage);
		return failCleanup ? Promise.ofException(STORAGE_EXCEPTION) : storage.cleanup(partitionsToDelete);
	}
}
