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

package io.activej.aggregation.measure;

import io.activej.common.Checks;
import io.activej.common.HashUtils;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.exp;
import static java.lang.Math.log;

public final class HyperLogLog implements Comparable<HyperLogLog> {
	private static final boolean CHECK = Checks.isEnabled(HyperLogLog.class);

	private final byte[] registers;

	public HyperLogLog(int registers) {
		this.registers = new byte[registers];
	}

	public HyperLogLog(byte[] registers) {
		this.registers = registers;
	}

	public byte[] getRegisters() {
		return registers;
	}

	public static HyperLogLog union(HyperLogLog a, HyperLogLog b) {
		if (CHECK) checkArgument(a.registers.length == b.registers.length, "Registers length mismatch");
		byte[] buckets = new byte[a.registers.length];
		for (int i = 0; i < a.registers.length; i++) {
			buckets[i] = a.registers[i] > b.registers[i] ? a.registers[i] : b.registers[i];
		}
		return new HyperLogLog(buckets);
	}

	public void union(HyperLogLog another) {
		if (CHECK) checkArgument(this.registers.length == another.registers.length, "Registers length mismatch");
		for (int i = 0; i < this.registers.length; i++) {
			byte thisValue = this.registers[i];
			byte thatValue = another.registers[i];
			if (thatValue > thisValue) {
				this.registers[i] = thatValue;
			}
		}
	}

	public void addToRegister(int register, int valueHash) {
		int zeros = Integer.numberOfTrailingZeros(valueHash) + 1;
		if (registers[register] < zeros) {
			registers[register] = (byte) zeros;
		}
	}

	public void addLongHash(long longHash) {
		addToRegister((int) longHash & (registers.length - 1), (int) (longHash >>> 32));
	}

	public void addObject(Object item) {
		addInt(item.hashCode());
	}

	public void addLong(long value) {
		addLongHash(HashUtils.murmur3hash(value));
	}

	public void addInt(int value) {
		addLongHash(HashUtils.murmur3hash((long) value));
	}

	private static final double ALPHA_16 = 0.673 * 16 * 16;
	private static final double ALPHA_32 = 0.697 * 32 * 32;
	private static final double ALPHA_64 = 0.709 * 64 * 64;
	private static final double ALPHA_XX = 0.7213;
	private static final double NLOG2 = -log(2.0);

	public int estimate() {
		int m = registers.length;
		double alpha;
		if (m == 16) {
			alpha = ALPHA_16;
		} else if (m == 32) {
			alpha = ALPHA_32;
		} else if (m == 64) {
			alpha = ALPHA_64;
		} else {
			alpha = ALPHA_XX / (1 + 1.079 / m) * m * m;
		}

		double sum = 0;
		for (byte value : registers) {
			sum += exp(value * NLOG2);
		}
		double estimate = alpha / sum;

		if (estimate < 5.0 / 2.0 * m) {
			int zeroCount = 0;
			for (byte bucket : registers) {
				if (bucket == 0)
					zeroCount++;
			}
			if (zeroCount != 0) {
				estimate = m * log((double) m / zeroCount);
			}
		}

		return (int) estimate;
	}

	@Override
	public int compareTo(HyperLogLog that) {
		return Integer.compare(this.estimate(), that.estimate());
	}
}
