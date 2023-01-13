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

package io.activej.crdt.primitives;

import io.activej.common.Checks;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;

import java.util.Arrays;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.max;

public final class GCounterLong implements CrdtMergable<GCounterLong> {
	private static final boolean CHECKS = Checks.isEnabled(GCounterLong.class);

	public static final BinarySerializer<GCounterLong> SERIALIZER = new Serializer();

	private final long[] state;

	private GCounterLong(long[] state) {
		this.state = state;
	}

	public GCounterLong(int n) {
		this(new long[n]);
	}

	public void increment(int id) {
		state[id]++;
	}

	public long value() {
		long v = 0;
		for (long c : state) {
			v += c;
		}
		return v;
	}

	@Override
	public GCounterLong merge(GCounterLong other) {
		if (CHECKS) checkArgument(state.length == other.state.length);
		long[] newState = new long[state.length];
		for (int i = 0; i < newState.length; i++) {
			newState[i] = max(state[i], other.state[i]);
		}
		return new GCounterLong(newState);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		return Arrays.equals(state, ((GCounterLong) o).state);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(state);
	}

	@Override
	public String toString() {
		return Long.toString(value());
	}

	private static class Serializer implements BinarySerializer<GCounterLong> {
		@Override
		public void encode(BinaryOutput out, GCounterLong item) {
			long[] state = item.state;
			out.writeVarInt(state.length);
			for (long c : state) {
				out.writeLong(c);
			}
		}

		@Override
		public GCounterLong decode(BinaryInput in) {
			long[] state = new long[in.readVarInt()];
			for (int i = 0; i < state.length; i++) {
				state[i] = in.readLong();
			}
			return new GCounterLong(state);
		}
	}
}
