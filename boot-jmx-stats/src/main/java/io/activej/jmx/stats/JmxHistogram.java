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

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.activej.common.Preconditions.checkArgument;

public interface JmxHistogram {
	int[] POWERS_OF_TWO =
			{
					0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
					262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728,
					268435456, 536870912, 1073741824
			};

	int[] POWERS_OF_TEN =
			{
					0, 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
			};

	int[] POWERS_OF_TEN_LINEAR =
			{
					0,
					1, 2, 3, 4, 5, 6, 7, 8, 9,
					10, 20, 30, 40, 50, 60, 70, 80, 90,
					100, 200, 300, 400, 500, 600, 700, 800, 900,
					1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
					10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000,
					100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000,
					1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000,
					10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000,
					100000000, 200000000, 300000000, 400000000, 500000000, 600000000, 700000000, 800000000, 900000000,
					1000000000, 2000000000
			};

	static JmxHistogram ofLevels(int[] levels) {
		checkArgument(levels.length > 0, "levels amount must be at least 1");
		for (int i = 1; i < levels.length; i++) {
			checkArgument(levels[i] > levels[i - 1], "levels must be ascending");
		}

		if (Arrays.equals(POWERS_OF_TWO, levels)) return new JmxHistograms.Base2();
		if (Arrays.equals(POWERS_OF_TEN, levels)) return new JmxHistograms.Base10();
		if (Arrays.equals(POWERS_OF_TEN_LINEAR, levels)) return new JmxHistograms.Base10Linear();

		return new JmxHistograms.Custom(levels);
	}

	int[] levels();

	long[] counts();

	void record(int value);

	void reset();

	JmxHistogram createAccumulator();

	void add(@NotNull JmxHistogram another);
}
