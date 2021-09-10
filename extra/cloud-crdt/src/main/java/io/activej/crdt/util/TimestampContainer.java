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

package io.activej.crdt.util;

import io.activej.crdt.function.CrdtFunction;
import io.activej.eventloop.Eventloop;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;

import java.util.function.BinaryOperator;

public final class TimestampContainer<S> {
	private final long timestamp;
	private final S state;

	public TimestampContainer(long timestamp, S state) {
		this.timestamp = timestamp;
		this.state = state;
	}

	public static <S> TimestampContainer<S> now(S state) {
		return new TimestampContainer<>(Eventloop.getCurrentEventloop().currentTimeMillis(), state);
	}

	public long getTimestamp() {
		return timestamp;
	}

	public S getState() {
		return state;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TimestampContainer<?> that = (TimestampContainer<?>) o;

		return timestamp == that.timestamp && state.equals(that.state);
	}

	@Override
	public int hashCode() {
		return 31 * (int) (timestamp ^ (timestamp >>> 32)) + state.hashCode();
	}

	@Override
	public String toString() {
		return "[" + state + "](ts=" + timestamp + ')';
	}

	public static <S> CrdtFunction<TimestampContainer<S>> createCrdtFunction(BinaryOperator<S> combiner) {
		return new CrdtFunction<TimestampContainer<S>>() {
			@Override
			public TimestampContainer<S> merge(TimestampContainer<S> first, TimestampContainer<S> second) {
				return new TimestampContainer<>(Math.max(first.getTimestamp(), second.getTimestamp()), combiner.apply(first.getState(), second.getState()));
			}

			@Override
			public @Nullable TimestampContainer<S> extract(TimestampContainer<S> state, long timestamp) {
				if (state.getTimestamp() > timestamp) {
					return state;
				}
				return null;
			}
		};
	}

	public static <S> BinarySerializer<TimestampContainer<S>> createSerializer(BinarySerializer<S> stateSerializer) {
		return new BinarySerializer<TimestampContainer<S>>() {
			@Override
			public void encode(BinaryOutput out, TimestampContainer<S> item) {
				out.writeLong(item.getTimestamp());
				stateSerializer.encode(out, item.getState());
			}

			@Override
			public TimestampContainer<S> decode(BinaryInput in) {
				return new TimestampContainer<>(in.readLong(), stateSerializer.decode(in));
			}
		};
	}
}
