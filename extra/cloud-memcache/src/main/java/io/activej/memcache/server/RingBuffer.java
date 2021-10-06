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

package io.activej.memcache.server;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.ObjectLongHashMap;
import io.activej.common.Checks;
import io.activej.common.initializer.WithInitializer;
import io.activej.jmx.stats.EventStats;
import io.activej.memcache.protocol.MemcacheRpcMessage.Slice;

import java.time.Duration;
import java.util.Arrays;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.StringFormatUtils.formatDuration;
import static io.activej.jmx.stats.MBeanFormat.formatTimestamp;
import static java.lang.System.currentTimeMillis;

/**
 * The implementation to handle the big amount of data
 * It works like a cache, when you use it you shouldn't rely on the result,
 * because it can be rewritten by the new data when it overfills
 */
public final class RingBuffer implements RingBufferMBean, WithInitializer<RingBuffer> {
	private static final boolean CHECK = Checks.isEnabled(RingBuffer.class);

	/**
	 * The main class for the caching the byte-arrays
	 */
	private static class Buffer {
		private final byte[] array;
		private final IntLongHashMap indexInt = new IntLongHashMap();
		private final LongLongHashMap indexLong = new LongLongHashMap();
		private final ObjectLongHashMap<byte[]> indexBytes = new ObjectLongHashMap<byte[]>() {
			@Override
			protected int hashKey(byte[] key) {
				int result = 0;
				for (byte element : key) {
					result = 92821 * result + element;
				}
				return result;
			}

			@Override
			protected boolean equals(Object v1, Object v2) {
				return Arrays.equals((byte[]) v1, (byte[]) v2);
			}
		};

		private int position = 0;
		private long timestamp;

		Buffer(int capacity) {
			this.array = new byte[capacity];
			this.timestamp = currentTimeMillis();
		}

		void clear() {
			indexInt.clear();
			indexLong.clear();
			indexBytes.clear();
			position = 0;
			timestamp = currentTimeMillis();
		}

		static int intValueOf(byte[] bytes) {
			return ((bytes[0] << 24)) |
					(bytes[1] & 0xff) << 16 |
					(bytes[2] & 0xff) << 8 |
					(bytes[3] & 0xff);
		}

		static long longValueOf(byte[] bytes) {
			return ((((long) bytes[0]) << 56) |
					(((long) bytes[1] & 0xff) << 48) |
					(((long) bytes[2] & 0xff) << 40) |
					(((long) bytes[3] & 0xff) << 32) |
					(((long) bytes[4] & 0xff) << 24) |
					(((long) bytes[5] & 0xff) << 16) |
					(((long) bytes[6] & 0xff) << 8) |
					(((long) bytes[7] & 0xff)));
		}

		Slice get(byte[] key) {
			long segment;
			if (key.length == 4) {
				segment = indexInt.getOrDefault(intValueOf(key), -1L);
			} else if (key.length == 8) {
				segment = indexLong.getOrDefault(longValueOf(key), -1L);
			} else {
				segment = indexBytes.getOrDefault(key, -1L);
			}
			if (segment < 0)
				return null;
			int offset = (int) (segment);
			int size = (int) (segment >>> 32);
			return new Slice(array, offset, size);
		}

		void put(byte[] key, byte[] data, int offset, int length) {
			assert length <= remaining();
			long segment = ((long) length << 32) | position;
			if (key.length == 4) {
				indexInt.put(intValueOf(key), segment);
			} else if (key.length == 8) {
				indexLong.put(longValueOf(key), segment);
			} else {
				indexBytes.put(key, segment);
			}
			System.arraycopy(data, offset, array, position, length);
			position += length;
		}

		int remaining() {
			return array.length - position;
		}

		long getTimestamp() {
			return timestamp;
		}

		int items() {
			return indexInt.size() + indexLong.size() + indexBytes.size();
		}
	}

	private final Buffer[] ringBuffers;
	private int currentBuffer = 0;

	// JMX
	private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private final EventStats statsPuts = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats statsGets = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats statsMisses = EventStats.create(SMOOTHING_WINDOW);
	private int countCycles = 0;

	public static RingBuffer create(int amountBuffers, int bufferCapacity) {
		checkArgument(amountBuffers > 0, "Amount of buffers should be greater than 0");
		checkArgument(bufferCapacity > 0, "Buffer capacity should be greater than 0");
		Buffer[] ringBuffers = new Buffer[amountBuffers];
		for (int i = 0; i < amountBuffers; i++) {
			ringBuffers[i] = new Buffer(bufferCapacity);
		}
		return new RingBuffer(ringBuffers);
	}

	private RingBuffer(Buffer[] ringBuffers) {
		this.ringBuffers = ringBuffers;
	}

	/**
	 * The method is used to try to get the slice from the {@link Buffer}
	 * It will return the latest actual data for the {@code key}
	 *
	 * @param key of your item
	 * @return the item in case your item is still present in {@link Buffer}
	 */
	public Slice get(byte[] key) {
		statsGets.recordEvent();
		for (int i = 0; i < ringBuffers.length; i++) {
			int current = currentBuffer - i;
			if (current < 0)
				current = ringBuffers.length + current;
			Slice slice = ringBuffers[current].get(key);
			if (slice != null) {
				return slice;
			}
		}
		statsMisses.recordEvent();
		return null;
	}

	/**
	 * The method is used to cache the actual information for the {@code key}
	 *
	 * @param key  is used as a pointer for the cached {@code data}
	 * @param data is thing to need to cache
	 */
	public void put(byte[] key, byte[] data) {
		put(key, data, 0, data.length);
	}

	/**
	 * The same to the above method,
	 * there are extra params to handle the {@code data}
	 */
	public void put(byte[] key, byte[] data, int offset, int length) {
		if (CHECK) checkArgument(data.length <= ringBuffers[currentBuffer].array.length,
				"Size of data is larger than the size of buffer");
		statsPuts.recordEvent();
		if (ringBuffers[currentBuffer].remaining() < length) {
			if (currentBuffer == ringBuffers.length - 1) {
				countCycles++;
			}
			currentBuffer = (currentBuffer + 1) % ringBuffers.length;
			ringBuffers[currentBuffer].clear();
		}
		ringBuffers[currentBuffer].put(key, data, offset, length);
	}

	private long getLifetimeMillis() {
		return currentTimeMillis() - ringBuffers[(currentBuffer + 1) % ringBuffers.length].getTimestamp();
	}

	// JMX
	@Override
	public void reset() {
		countCycles = 0;
		statsMisses.resetStats();
	}

	@Override
	public String getStatsPuts() {
		return statsPuts.toString();
	}

	@Override
	public double getStatsPutsRate() {
		return statsPuts.getSmoothedRate();
	}

	@Override
	public long getStatsPutsTotal() {
		return statsPuts.getTotalCount();
	}

	@Override
	public String getStatsGets() {
		return statsGets.toString();
	}

	@Override
	public double getStatsGetsRate() {
		return statsGets.getSmoothedRate();
	}

	@Override
	public long getStatsGetsTotal() {
		return statsGets.getTotalCount();
	}

	@Override
	public String getStatsMisses() {
		return statsMisses.toString();
	}

	@Override
	public double getStatsMissesRate() {
		return statsMisses.getSmoothedRate();
	}

	@Override
	public long getStatsMissesTotal() {
		return statsMisses.getTotalCount();
	}

	/**
	 * Is used to figure out the amount of byte[] arrays which are stored
	 *
	 * @return amount of stored data
	 */
	@Override
	public int getItems() {
		int items = 0;
		for (Buffer ringBuffer : ringBuffers) {
			items += ringBuffer.items();
		}
		return items;
	}

	/**
	 * Is used to get the occupied capacity
	 *
	 * @return amount of occupied capacity
	 */
	@Override
	public long getSize() {
		long size = 0;
		for (Buffer ringBuffer : ringBuffers) {
			size += ringBuffer.position;
		}
		return size;
	}

	@Override
	public String getLifetime() {
		return formatDuration(Duration.ofMillis(getLifetimeMillis()));
	}

	@Override
	public long getLifetimeSeconds() {
		return getLifetimeMillis() / 1000;
	}

	@Override
	public String getCurrentBuffer() {
		return (currentBuffer + 1) + " / " + ringBuffers.length + " @ " +
				formatTimestamp(ringBuffers[currentBuffer].getTimestamp());
	}

	@Override
	public int getFullCycles() {
		return countCycles;
	}
}
