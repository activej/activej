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

package io.activej.bytebuf;

import io.activej.common.ApplicationSettings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Optimized lock-free concurrent queue implementation for the {@link ByteBuf ByteBufs} that is used in {@link ByteBufPool}
 */
public final class ByteBufConcurrentQueue {
	private static final boolean YIELD = ApplicationSettings.getBoolean(ByteBufConcurrentQueue.class, "yield", true);

	final AtomicInteger realMin = new AtomicInteger(0);

	private volatile Ring ring = new Ring(1);

	public ByteBuf poll() {
		Ring ring = this.ring;
		return ring.poll();
	}

	public void offer(ByteBuf item) {
		Ring ring = this.ring;
		if (ring.offer(item)) return;
		grow(item, ring);
	}

	private void grow(ByteBuf item, Ring ring) {
		Ring ringNew = new Ring(ring.length * 2);
		this.ring = ringNew;
		ringNew.offer(item);
		while (true) {
			item = ring.poll();
			if (item == null) break;
			ringNew.offer(item);
		}
	}

	final class Ring {
		private final AtomicLong pos = new AtomicLong(0);
		private final AtomicReferenceArray<ByteBuf> items;
		private final int length;
		private final int mask;

		Ring(int items) {
			this.items = new AtomicReferenceArray<>(items);
			this.length = this.items.length();
			this.mask = this.length - 1;
		}

		public ByteBuf poll() {
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
					if (YIELD) Thread.yield();
					continue;
				}
				break;
			} while (true);

			ByteBuf item;
			do {
				item = items.getAndSet(tail & mask, null);
				if (item == null) {
					continue;
				}
				break;
			} while (true);

			return item;
		}

		public boolean offer(ByteBuf item) {
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
					if (YIELD) Thread.yield();
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

			if (ByteBufPool.USE_WATCHDOG) {
				int size = head - tail;
				ByteBufConcurrentQueue.this.realMin.updateAndGet(prevMin -> Math.min(prevMin, size));
			}

			return true;
		}
	}

	public void clear() {
		while (!isEmpty()) {
			poll();
		}
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		long pos1 = ring.pos.get();
		int head = (int) (pos1 >>> 32);
		int tail = (int) pos1;
		return head - tail;
	}

	@Override
	public String toString() {
		return
			"ByteBufConcurrentQueue{" +
			"size=" + size() +
			'}';
	}
}
