package io.activej.datastream.consumer;

import io.activej.common.ApplicationSettings;
import io.activej.datastream.supplier.StreamDataAcceptor;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

public final class BlockingStreamConsumer<T> extends AbstractStreamConsumer<T> {
	public static final int DEFAULT_BUFFER_SIZE = ApplicationSettings.getInt(BlockingStreamConsumer.class, "bufferSize", 8192);

	private final Queue queue;

	private volatile boolean endOfStream;

	private BlockingStreamConsumer(int bufferSize) {
		checkArgument(bufferSize > 0, "Negative buffer size");
		this.queue = new Queue(bufferSize);
	}

	public static <T> BlockingStreamConsumer<T> create() {
		return new BlockingStreamConsumer<>(DEFAULT_BUFFER_SIZE);
	}

	public static <T> BlockingStreamConsumer<T> create(int bufferSize) {
		return new BlockingStreamConsumer<>(bufferSize);
	}

	public int getBufferCapacity() {
		return queue.capacity();
	}

	public int getBufferSize() {
		return queue.size();
	}

	public @Nullable T take() throws InterruptedException, ExecutionException {
		checkState(!endOfStream);

		T item = queue.take();
		if (item != null) {
			return item;
		}

		endOfStream = true;
		if (isException()) {
			throw new ExecutionException(getAcknowledgement().getException());
		}

		return null;
	}

	public CompletableFuture<Void> submitAcknowledgement() {
		return reactor.submit(() -> {
			acknowledge();
			return getAcknowledgement();
		});
	}

	@Override
	protected void onInit() {
		queue.onRequestMoreData();
	}

	@Override
	protected void onEndOfStream() {
		queue.endOfStream();
	}

	@Override
	protected void onError(Exception e) {
		queue.close();
	}

	public class Queue extends BlockingTakeQueue<T> implements StreamDataAcceptor<T> {
		public Queue(int bufferSize) {
			super(bufferSize);
		}

		@Override
		protected void onRequestMoreData() {
			resume(this);
		}

		public final void accept(T item) {
			if (put(item)) {
				suspend();
			}
		}
	}
}
