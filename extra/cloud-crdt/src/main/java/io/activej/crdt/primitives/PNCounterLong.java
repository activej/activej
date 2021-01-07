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

public final class PNCounterLong implements CrdtMergable<PNCounterLong> {
	public static final BinarySerializer<PNCounterLong> SERIALIZER = new Serializer();

	private final GCounterLong p;
	private final GCounterLong n;

	private PNCounterLong(GCounterLong p, GCounterLong n) {
		this.p = p;
		this.n = n;
	}

	public PNCounterLong(int nodes) {
		this(new GCounterLong(nodes), new GCounterLong(nodes));
	}

	public void increment(int id) {
		p.increment(id);
	}

	public void decrement(int id) {
		n.increment(id);
	}

	public long value() {
		return p.value() - n.value();
	}

	@Override
	public PNCounterLong merge(PNCounterLong other) {
		return new PNCounterLong(p.merge(other.p), n.merge(other.n));
	}

	@Override
	public String toString() {
		return Long.toString(value());
	}

	private static class Serializer implements BinarySerializer<PNCounterLong> {
		@Override
		public void encode(BinaryOutput out, PNCounterLong item) {
			GCounterLong.SERIALIZER.encode(out, item.p);
			GCounterLong.SERIALIZER.encode(out, item.n);
		}

		@Override
		public PNCounterLong decode(BinaryInput in) {
			return new PNCounterLong(GCounterLong.SERIALIZER.decode(in), GCounterLong.SERIALIZER.decode(in));
		}
	}
}
