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

package io.activej.datastream.processor.reducer;

import io.activej.common.annotation.StaticFactories;
import io.activej.datastream.processor.reducer.impl.*;

/**
 * Static utility methods pertaining to {@link Reducer}.
 * Contains primary ready for use reducers.
 */
@StaticFactories(Reducer.class)
public class Reducers {

	/**
	 * Returns reducer which streams only one element from group of same keys.
	 *
	 * @param <K> type of key
	 * @param <T> type of output
	 */
	public static <K, T> Reducer<K, T, T, Void> deduplicateReducer() {
		return new Deduplicate<>();
	}

	/**
	 * Returns reducer which streams all receives elements sorted by keys.
	 *
	 * @param <K> type of key
	 * @param <T> type of output
	 */
	public static <K, T> Reducer<K, T, T, Void> mergeReducer() {
		return new Merge<>();
	}

	/**
	 * Creates a new reducer which receives items,accumulated it, produces after end of stream
	 * and streams result
	 */
	public static <K, I, O, A> Reducer<K, I, O, A> inputToOutput(ReducerToResult<K, I, O, A> reducerToResult) {
		return new InputToOutput<>(reducerToResult);
	}

	/**
	 * Creates a new reducer which receives items,accumulated it and streams obtained accumulator
	 */
	public static <K, I, O, A> Reducer<K, I, A, A> inputToAccumulator(ReducerToResult<K, I, O, A> reducerToResult) {
		return new InputToAccumulator<>(reducerToResult);
	}

	/**
	 * Creates a new reducer which receives accumulators,combines it, produces after end of stream
	 * and streams result
	 */
	public final <K, I, O, A> Reducer<K, A, O, A> accumulatorToOutput(ReducerToResult<K, I, O, A> reducerToResult) {
		return new AccumulatorToOutput<>(reducerToResult);
	}

	/**
	 * Creates a new reducer which receives accumulators,combines it, and streams obtained accumulator
	 */
	public static <K, I, O, A> Reducer<K, A, A, A> accumulatorToAccumulator(ReducerToResult<K, I, O, A> reducerToResult) {
		return new AccumulatorToAccumulator<>(reducerToResult);
	}
}
