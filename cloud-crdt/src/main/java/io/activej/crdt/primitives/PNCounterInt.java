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

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;

public final class PNCounterInt implements CrdtMergable<PNCounterInt> {
	public static final BinarySerializer<PNCounterInt> SERIALIZER = new Serializer();

	private final GCounterInt p;
	private final GCounterInt n;

	private PNCounterInt(GCounterInt p, GCounterInt n) {
		this.p = p;
		this.n = n;
	}

	public PNCounterInt(int nodes) {
		this(new GCounterInt(nodes), new GCounterInt(nodes));
	}

	public void increment(int id) {
		p.increment(id);
	}

	public void decrement(int id) {
		n.increment(id);
	}

	public int value() {
		return p.value() - n.value();
	}

	@Override
	public PNCounterInt merge(PNCounterInt other) {
		return new PNCounterInt(p.merge(other.p), n.merge(other.n));
	}

	@Override
	public String toString() {
		return Integer.toString(value());
	}

	private static class Serializer implements BinarySerializer<PNCounterInt> {
		@Override
		public void encode(BinaryOutput out, PNCounterInt item) {
			GCounterInt.SERIALIZER.encode(out, item.p);
			GCounterInt.SERIALIZER.encode(out, item.n);
		}

		@Override
		public PNCounterInt decode(BinaryInput in) {
			return new PNCounterInt(GCounterInt.SERIALIZER.decode(in), GCounterInt.SERIALIZER.decode(in));
		}
	}
}
