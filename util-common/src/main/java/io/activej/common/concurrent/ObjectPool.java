/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.common.concurrent;

import io.activej.common.ApplicationSettings;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Integer.numberOfLeadingZeros;

/**
 * Optimized lock-free concurrent object pool implementation
 */
public final class ObjectPool<T> {
	private static final int PARK_NANOS = ApplicationSettings.getInt(ObjectPool.class, "parkNanos", 1);
	private static final int INITIAL_CAPACITY = ApplicationSettings.getInt(ObjectPool.class, "initialCapacity", 1);

	private volatile Ring<T> ring;
	private final @Nullable Supplier<T> supplier;

	public ObjectPool() {
		this(INITIAL_CAPACITY, null);
	}

	public ObjectPool(int initialCapacity) {
		this(initialCapacity, null);
	}

	public ObjectPool(@Nullable Supplier<T> supplier) {
		this(INITIAL_CAPACITY, supplier);
	}

	public ObjectPool(int initialCapacity, @Nullable Supplier<T> supplier) {
		checkArgument(initialCapacity == 1 << 32 - numberOfLeadingZeros(initialCapacity - 1),
			"initialCapacity must be a power of 2");
		this.ring = new Ring<>(initialCapacity);
		this.supplier = supplier;
	}

	public T poll() {
		Ring<T> ring = this.ring;
		return ring.poll();
	}

	public T ensure() {
		if (supplier == null) throw new UnsupportedOperationException();
		T item = poll();
		return item != null ? item : supplier.get();
	}

	public T ensure(Supplier<T> supplier) {
		if (supplier == null) throw new NullPointerException();
		T item = poll();
		return item != null ? item : supplier.get();
	}

	public void offer(T item) {
		Ring<T> ring = this.ring;
		if (ring.offer(item)) return;
		grow(item, ring);
	}

	private synchronized void grow(T item, Ring<T> ring) {
		if (ring == this.ring) {
			this.ring = new Ring<>(ring.length * 2);
		}
		this.ring.offer(item);
		while (true) {
			item = ring.poll();
			if (item == null) break;
			this.ring.offer(item);
		}
	}

	private static final class Ring<T> {
		private final AtomicLong pos = new AtomicLong(0);
		private final AtomicReferenceArray<T> items;
		private final int length;
		private final int mask;

		Ring(int items) {
			this.items = new AtomicReferenceArray<>(items);
			this.length = this.items.length();
			this.mask = this.length - 1;
		}

		public T poll() {
			long pos1, pos2;
			int head, tail;
			do {
				pos1 = pos.get();
				head = (int) (pos1 >>> 32);
				tail = (int) pos1;
				if (head == tail) {
					return null;
				}
				pos2 = ((long) head << 32) + ((tail + 1) & 0xFFFFFFFFL);
				if (!pos.compareAndSet(pos1, pos2)) {
					LockSupport.parkNanos(PARK_NANOS);
					continue;
				}
				break;
			} while (true);

			T item;
			do {
				item = items.getAndSet(tail & mask, null);
				if (item == null) {
					continue;
				}
				break;
			} while (true);

			return item;
		}

		public boolean offer(T item) {
			long pos1, pos2;
			int head, tail;
			do {
				pos1 = pos.get();
				head = (int) (pos1 >>> 32);
				tail = (int) pos1;
				if (head == tail + length) {
					return false;
				}
				pos2 = pos1 + 0x100000000L;
				if (!pos.compareAndSet(pos1, pos2)) {
					LockSupport.parkNanos(PARK_NANOS);
					continue;
				}
				break;
			} while (true);

			do {
				item = items.getAndSet(head & mask, item);
				if (item != null) {
					continue;
				}
				break;
			} while (true);

			return true;
		}
	}

	public synchronized void clear() {
		ring = new Ring<>(ring.length);
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		long pos = ring.pos.get();
		int head = (int) (pos >>> 32);
		int tail = (int) pos;
		return head - tail;
	}

	public int capacity() {
		return ring.length;
	}

	@Override
	public String toString() {
		return
			"ObjectPool{" +
			"size=" + size() +
			'}';
	}
}
