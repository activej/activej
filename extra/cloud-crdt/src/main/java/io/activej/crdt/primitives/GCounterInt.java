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

public final class GCounterInt implements CrdtMergable<GCounterInt> {
	private static final boolean CHECKS = Checks.isEnabled(GCounterInt.class);

	public static final BinarySerializer<GCounterInt> SERIALIZER = new Serializer();

	private final int[] state;

	private GCounterInt(int[] state) {
		this.state = state;
	}

	public GCounterInt(int nodes) {
		this(new int[nodes]);
	}

	public void increment(int id) {
		state[id]++;
	}

	public int value() {
		int v = 0;
		for (int c : state) {
			v += c;
		}
		return v;
	}

	@Override
	public GCounterInt merge(GCounterInt other) {
		if (CHECKS) checkArgument(state.length == other.state.length);
		int[] newState = new int[state.length];
		for (int i = 0; i < newState.length; i++) {
			newState[i] = max(state[i], other.state[i]);
		}
		return new GCounterInt(newState);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		return Arrays.equals(state, ((GCounterInt) o).state);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(state);
	}

	@Override
	public String toString() {
		return Integer.toString(value());
	}

	private static class Serializer implements BinarySerializer<GCounterInt> {
		@Override
		public void encode(BinaryOutput out, GCounterInt item) {
			int[] state = item.state;
			out.writeVarInt(state.length);
			for (int c : state) {
				out.writeInt(c);
			}
		}

		@Override
		public GCounterInt decode(BinaryInput in) {
			int[] state = new int[in.readVarInt()];
			for (int i = 0; i < state.length; i++) {
				state[i] = in.readInt();
			}
			return new GCounterInt(state);
		}
	}
}
