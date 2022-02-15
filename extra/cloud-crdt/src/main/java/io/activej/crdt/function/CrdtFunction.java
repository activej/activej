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

package io.activej.crdt.function;

import io.activej.crdt.primitives.CrdtType;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public interface CrdtFunction<S> {

	/**
	 * This method should combine two given CRDT values together,
	 * not violating any of the CRDT conditions.
	 */
	S merge(S first, long firstTimestamp, S second, long secondTimestamp);

	/**
	 * Extract partial CRDT state from given state, which contains only the
	 * changes to it since given timestamp.
	 * <p>
	 * If there were no changes, then this method <b>must</b> return <code>null</code>.
	 * <p>
	 * Suppose we have some CRDT value 'old', which is the state of something
	 * either exactly at or after the timestamp.
	 * <p>
	 * This method should create a CRDT value that, when combined with 'old',
	 * gives you 'state', which is 'old' updated to current time.
	 * <p>
	 * Basically this is almost like taking a CRDT diff between 'old' and 'state'.
	 * <p>
	 * It can be a huge optimization e.g. for big CRDT maps:
	 * <p>
	 * The whole map state could contain thousands of key-value pairs,
	 * and instead of combining 'old' with 'state' (which with CRDT would achieve the most complete map)
	 * that requires serializing, transferring and/or storing the whole 'state',
	 * one could extract only the entries that are changed between 'old' and 'state',
	 * serialize and transfer only those and then CRDT-combine those with 'old', achieving the
	 * same result with much less resources spent.
	 */
	@Nullable S extract(S state, long timestamp);

	static <S extends CrdtType<S>> CrdtFunction<S> ofCrdtType() {
		return new CrdtFunction<S>() {
			@Override
			public S merge(S first, long firstTimestamp, S second, long secondTimestamp) {
				return first.merge(second);
			}

			@Override
			public @Nullable S extract(S state, long timestamp) {
				return state.extract(timestamp);
			}
		};
	}

	static <S> CrdtFunction<S> ignoringTimestamp(BinaryOperator<S> mergeOperator) {
		return ignoringTimestamp(mergeOperator, (s, $) -> s);
	}

	static <S> CrdtFunction<S> ignoringTimestamp(BinaryOperator<S> mergeOperator, BiFunction<S, Long, S> extractFn) {
		return new CrdtFunction<S>() {
			@Override
			public S merge(S first, long firstTimestamp, S second, long secondTimestamp) {
				return mergeOperator.apply(first, second);
			}

			@Override
			public @Nullable S extract(S state, long timestamp) {
				return extractFn.apply(state, timestamp);
			}
		};
	}
}
