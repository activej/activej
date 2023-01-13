package io.activej.datastream;

import io.activej.common.Checks;
import io.activej.reactor.ImplicitlyReactive;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Integer.numberOfLeadingZeros;

public abstract class BlockingTakeQueue<T> extends ImplicitlyReactive {
	private static final boolean CHECK = Checks.isEnabled(BlockingTakeQueue.class);

	private final AtomicReferenceArray<T> queue;
	private final int mask;

	private int tail;
	private volatile int head;

	private volatile boolean closed;

	private volatile Thread takeThread;
	private final AtomicBoolean requestMoreData = new AtomicBoolean();

	public BlockingTakeQueue(int capacity) {
		checkArgument(capacity > 0, "Negative capacity");

		int nextPowerOf2 = 1 << (32 - numberOfLeadingZeros(capacity - 1));
		this.queue = new AtomicReferenceArray<>(nextPowerOf2);
		this.mask = nextPowerOf2 - 1;
	}

	public int size() {
		return tail - head;
	}

	public int capacity() {
		return queue.length();
	}

	public boolean isSaturated() {
		return size() == capacity();
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public boolean isClosed() {
		return closed;
	}

	public boolean put(T x) {
		if (CHECK) {
			checkInReactorThread(this);
			checkState(!closed);
			checkState(!isSaturated());
		}

		if (queue.getAndSet(tail++ & mask, x) != null) {
			throw new AssertionError();
		}

		LockSupport.unpark(takeThread);

		return isSaturated();
	}

	public synchronized @Nullable T take() throws InterruptedException {
		T x = queue.getAndSet(head & mask, null);
		if (x == null) {
			takeThread = Thread.currentThread();
			try {
				while ((x = queue.getAndSet(head & mask, null)) == null) {
					if (closed) {
						return null;
					}
					LockSupport.park();
					if (Thread.interrupted()) {
						throw new InterruptedException();
					}
				}
			} finally {
				takeThread = null;
			}
		}
		//noinspection ConstantConditions
		assert x != null;
		head++;

		requestMoreData();

		return x;
	}

	private void requestMoreData() {
		if (requestMoreData.compareAndSet(false, true)) {
			reactor.submit(() -> {
				if (closed) return;

				requestMoreData.set(false);
				if (!isSaturated()) {
					onRequestMoreData();
				}
			});
		}
	}

	protected abstract void onRequestMoreData();

	public void endOfStream() {
		checkInReactorThread(this);

		closed = true;
		LockSupport.unpark(takeThread);
	}

	public void close() {
		for (int i = 0; i < queue.length(); i++) {
			queue.set(i, null);
		}
		endOfStream();
	}
}
