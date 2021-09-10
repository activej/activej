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
import org.jetbrains.annotations.Nullable;

public class LWWObject<T> implements CrdtType<LWWObject<T>> {
	private final long timestamp;
	private final T object;

	public LWWObject(long timestamp, T object) {
		this.timestamp = timestamp;
		this.object = object;
	}

	public T getObject() {
		return object;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public LWWObject<T> merge(LWWObject<T> other) {
		if (timestamp < other.timestamp) {
			return other;
		}
		return this;
	}

	@Override
	public @Nullable LWWObject<T> extract(long timestamp) {
		if (timestamp >= this.timestamp) {
			return null;
		}
		return this;
	}

	public static class Serializer<T> implements BinarySerializer<LWWObject<T>> {
		private final BinarySerializer<T> valueSerializer;

		public Serializer(BinarySerializer<T> valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

		@Override
		public void encode(BinaryOutput out, LWWObject<T> item) {
			out.writeLong(item.timestamp);
			valueSerializer.encode(out, item.object);
		}

		@Override
		public LWWObject<T> decode(BinaryInput in) {
			return new LWWObject<>(in.readLong(), valueSerializer.decode(in));
		}
	}
}
