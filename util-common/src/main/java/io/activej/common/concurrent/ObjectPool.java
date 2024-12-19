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
 * A lock-free, concurrent object pool implementation.
 *
 * <p>This class provides a pool of reusable objects, allowing for efficient reuse of objects to reduce
 * of repeatedly allocating and deallocating memory.
 *
 * <p>The object pool is implemented using a ring buffer for storing
 * objects and can be expanded if needed.</p>
 *
 * @param <T> the type of objects managed by this pool
 */
public final class ObjectPool<T> {
	private static final int PARK_NANOS = ApplicationSettings.getInt(ObjectPool.class, "parkNanos", 1);
	private static final int INITIAL_CAPACITY = ApplicationSettings.getInt(ObjectPool.class, "initialCapacity", 1);

	private volatile Ring<T> ring;
	private final @Nullable Supplier<T> supplier;

	/**
	 * Creates an object pool with an initial capacity.
	 */
	public ObjectPool() {
		this(INITIAL_CAPACITY, null);
	}

	/**
	 * Creates an object pool with a specified initial capacity.
	 *
	 * @param initialCapacity the initial capacity of the object pool. Must be a power of 2.
	 */
	public ObjectPool(int initialCapacity) {
		this(initialCapacity, null);
	}

	/**
	 * Creates an object pool with an initial capacity and a supplier to create new instances if none are available.
	 *
	 * @param supplier a supplier function used to create new instances if needed
	 */
	public ObjectPool(@Nullable Supplier<T> supplier) {
		this(INITIAL_CAPACITY, supplier);
	}

	/**
	 * Creates an object pool with a specified initial capacity and a supplier.
	 *
	 * @param initialCapacity the initial capacity of the object pool. Must be a power of 2.
	 * @param supplier a supplier function used to create new instances if needed
	 */
	public ObjectPool(int initialCapacity, @Nullable Supplier<T> supplier) {
		checkArgument(initialCapacity == 1 << 32 - numberOfLeadingZeros(initialCapacity - 1),
			"initialCapacity must be a power of 2");
		this.ring = new Ring<>(initialCapacity);
		this.supplier = supplier;
	}

	/**
	 * Attempts to retrieve an object from the pool.
	 *
	 * @return an object from the pool if available, otherwise {@code null}
	 */
	public T poll() {
		Ring<T> ring = this.ring;
		return ring.poll();
	}

	/**
	 * Retrieves an object from the pool, or creates a new one using the default supplier if none are available.
	 *
	 * @return an object from the pool, or a newly created one
	 * @throws UnsupportedOperationException if no default supplier is provided
	 */
	public T ensure() {
		if (supplier == null) throw new UnsupportedOperationException();
		T item = poll();
		return item != null ? item : supplier.get();
	}

	/**
	 * Retrieves an object from the pool, or creates a new one using the specified supplier if none are available.
	 *
	 * @param supplier a supplier function used to create a new instance if needed
	 * @return an object from the pool, or a newly created one
	 * @throws NullPointerException if the supplier is {@code null}
	 */
	public T ensure(Supplier<T> supplier) {
		if (supplier == null) throw new NullPointerException();
		T item = poll();
		return item != null ? item : supplier.get();
	}

	/**
	 * Returns an object to the pool.
	 *
	 * <p>If the pool is full, the pool capacity is doubled automatically.
	 *
	 * @param item the object to return to the pool
	 */
	public void offer(T item) {
		Ring<T> ring = this.ring;
		if (ring.offer(item)) return;
		grow(item, ring);
	}

	/**
	 * Doubles the capacity of the pool and adds the specified item to the pool.
	 *
	 * @param item the object to be added to the pool
	 * @param ring the current ring buffer to be expanded
	 */
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

	/**
	 * Clears the object pool, removing all objects.
	 */
	public synchronized void clear() {
		ring = new Ring<>(ring.length);
	}

	/**
	 * Checks if the pool is empty.
	 *
	 * @return {@code true} if the pool is empty, {@code false} otherwise
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the current number of objects in the pool.
	 *
	 * @return the number of objects in the pool
	 */
	public int size() {
		long pos = ring.pos.get();
		int head = (int) (pos >>> 32);
		int tail = (int) pos;
		return head - tail;
	}

	/**
	 * Returns the current capacity of the pool.
	 *
	 * @return the current capacity of the pool
	 */
	public int capacity() {
		return ring.length;
	}

	/**
	 * Returns a string representation of the object pool.
	 *
	 * @return a string representation of the pool, including the current size
	 */
	@Override
	public String toString() {
		return
			"ObjectPool{" +
			"size=" + size() +
			'}';
	}

	/**
	 * A helper class represents a ring buffer for storing objects in the object pool.
	 *
	 * @param <T> the type of objects stored in the ring buffer
	 */
	private static final class Ring<T> {
		private final AtomicLong pos = new AtomicLong(0);
		private final AtomicReferenceArray<T> items;
		private final int length;
		private final int mask;

		/**
		 * Creates a new ring buffer with the specified size.
		 *
		 * @param items the capacity of the ring buffer
		 */
		Ring(int items) {
			this.items = new AtomicReferenceArray<>(items);
			this.length = this.items.length();
			this.mask = this.length - 1;
		}

		/**
		 * Attempts to retrieve an object from the ring buffer.
		 *
		 * @return an object from the buffer if available, otherwise {@code null}
		 */
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

		/**
		 * Attempts to add an object to the ring buffer.
		 *
		 * @param item the object to add to the buffer
		 * @return {@code true} if the object was successfully added, {@code false} if the buffer is full
		 */
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
}
