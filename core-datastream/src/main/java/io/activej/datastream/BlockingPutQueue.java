package io.activej.datastream;

import io.activej.common.Checks;
import io.activej.reactor.ImplicitlyReactive;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Integer.numberOfLeadingZeros;

public abstract class BlockingPutQueue<T> extends ImplicitlyReactive {
	private static final boolean CHECKS = Checks.isEnabled(BlockingPutQueue.class);

	private final AtomicReferenceArray<T> queue;
	private final int mask;

	private volatile int tail;
	private int head;

	private volatile boolean closed;

	private volatile Thread putThread;
	private final AtomicBoolean hasMoreData = new AtomicBoolean();

	public BlockingPutQueue(int capacity) {
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

	public synchronized void put(T x) throws InterruptedException {
		if (closed) return;

		if (!queue.compareAndSet(tail & mask, null, x)) {
			putThread = Thread.currentThread();
			try {
				while (!queue.compareAndSet(tail & mask, null, x)) {
					if (closed) return;

					LockSupport.park();
					if (Thread.interrupted()) {
						throw new InterruptedException();
					}
				}
			} finally {
				putThread = null;
			}
		}

		tail++;

		requestTake();
	}

	public T take() {
		if (CHECKS) {
			checkInReactorThread(this);
			checkState(!isEmpty());
			checkState(!isClosed());
		}

		T item = queue.getAndSet(head++ & mask, null);

		assert item != null;

		LockSupport.unpark(putThread);

		return item;
	}

	private void requestTake() {
		if (closed) return;

		if (hasMoreData.compareAndSet(false, true)) {
			reactor.submit(() -> {
				if (closed) return;

				hasMoreData.set(false);
				if (!isEmpty()) {
					onMoreData();
				}
				if (!isEmpty()) {
					reactor.postLast(this::requestTake);
				}
			});
		}
	}

	protected abstract void onMoreData();

	public void close() {
		checkInReactorThread(this);
		closed = true;
		for (int i = 0; i < queue.length(); i++) {
			queue.set(i, null);
		}

		LockSupport.unpark(putThread);
	}
}
