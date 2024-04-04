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

package io.activej.jmx.stats;

import java.util.Arrays;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Math.abs;
import static java.util.Arrays.binarySearch;

public final class JmxHistograms {
	private static final long[] TABLE_10 =
		{
			1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L, 1000000000L,
			10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L,
			1000000000000000L, 10000000000000000L, 100000000000000000L, 1000000000000000000L
		};

	static int longLogBase10(long v) {
		int t = (64 - numberOfLeadingZeros(v)) * 1233 >>> 12;
		return t - (v < TABLE_10[t] ? 1 : 0);
	}

	public abstract static class AbstractJmxHistogram implements JmxHistogram {
		protected final long[] counters;
		protected final long[] levels;

		protected AbstractJmxHistogram(long[] levels) {
			this.counters = new long[levels.length + 1];
			this.levels = levels;
		}

		protected AbstractJmxHistogram(long[] levels, int counters) {
			this.counters = new long[counters];
			this.levels = levels;
		}

		@Override
		public long[] levels() {
			return levels;
		}

		@Override
		public long[] counts() {
			long[] result = new long[levels.length + 1];
			for (int i = 0; i <= levels.length; i++) {
				result[i] = getResult(i);
			}
			return result;
		}

		protected abstract long getResult(int index);

		@Override
		public void reset() {
			Arrays.fill(counters, 0L);
		}

		@Override
		public void add(JmxHistogram another) {
			AbstractJmxHistogram anotherImpl = (AbstractJmxHistogram) another;
			checkArgument(Arrays.equals(anotherImpl.levels, levels) && anotherImpl.counters.length == counters.length, "Histograms mismatch");
			for (int i = 0; i < counters.length; i++) {
				this.counters[i] += anotherImpl.counters[i];
			}
		}
	}

	public static final class Base2 extends AbstractJmxHistogram {
		public Base2() {
			super(POWERS_OF_TWO);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Base2();
		}

		@Override
		public void record(long value) {
			counters[64 - numberOfLeadingZeros(value)]++;
		}

		@Override
		protected long getResult(int index) {
			return counters[(index + 65 - 1) % 65];
		}
	}

	public static class Base10 extends AbstractJmxHistogram {
		public Base10() {
			super(POWERS_OF_TEN);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Base10();
		}

		@Override
		public void record(long value) {
			if (value >= 0) {
				counters[longLogBase10(value) + 2]++;
			} else {
				counters[0]++;
			}
		}

		@Override
		protected long getResult(int index) {
			return counters[index];
		}
	}

	public static final class Base10Linear extends AbstractJmxHistogram {
		public Base10Linear() {
			super(POWERS_OF_TEN_LINEAR);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Base10Linear();
		}

		@Override
		public void record(long value) {
			if (value == 0) {
				counters[1]++;
			} else if (value > 0) {
				int index = (64 - numberOfLeadingZeros(value)) * 1233 >>> 12;
				long power = TABLE_10[index];
				if (value < power) {
					power = TABLE_10[--index];
				}
				int subindex = (int) (value / power);
				counters[index * 9 + subindex + 1]++;
			} else {
				counters[0]++;
			}
		}

		@Override
		protected long getResult(int index) {
			return counters[index];
		}
	}

	public static final class Custom extends AbstractJmxHistogram {
		public Custom(long[] levels) {
			super(levels);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Custom(levels);
		}

		@Override
		public void record(long value) {
			counters[abs(binarySearch(levels, value) + 1)]++;
		}

		@Override
		protected long getResult(int index) {
			return counters[index];
		}
	}

}
