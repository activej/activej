package io.activej.datastream;

import io.activej.common.ApplicationSettings;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SynchronousStreamConsumer<T> extends AbstractStreamConsumer<T> {
	public static final int DEFAULT_BUFFER_SIZE = ApplicationSettings.getInt(SynchronousStreamConsumer.class, "bufferSize", 8192);

	private static final Object FINISH_OBJECT = new Object();

	private final LinkedBlockingQueue<Object> streamQueue = new LinkedBlockingQueue<>();

	private @Nullable Exception error;

	private final AtomicBoolean suspended = new AtomicBoolean(false);

	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private boolean done;

	private SynchronousStreamConsumer() {
	}

	public static <T> SynchronousStreamConsumer<T> create() {
		return new SynchronousStreamConsumer<>();
	}

	public SynchronousStreamConsumer<T> withBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public Optional<T> fetch() {
		if (error != null) {
			throw new RuntimeException(error);
		}

		if (done) {
			throw new AssertionError();
		}

		Object item;
		try {
			item = streamQueue.take();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}

		if (item == FINISH_OBJECT) {
			if (error != null) {
				throw new RuntimeException(error);
			}

			done = true;
			reactor.submit(this::acknowledge);
			return Optional.empty();
		}
		if (suspended.compareAndSet(true, false)) {
			reactor.submit(this::doResume);
		}

		//noinspection unchecked
		return Optional.of(((T) item));
	}

	public boolean isDone() {
		return done;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	@Override
	protected void onInit() {
		doResume();
	}

	@Override
	protected void onError(Exception e) {
		error = e;
		streamQueue.add(FINISH_OBJECT);
	}

	private void doResume() {
		resume(item -> {
			if (streamQueue.size() > bufferSize) {
				suspend();
				suspended.set(true);
			}
			streamQueue.add(item);
		});
	}

	@Override
	protected void onEndOfStream() {
		streamQueue.add(FINISH_OBJECT);
	}
}
