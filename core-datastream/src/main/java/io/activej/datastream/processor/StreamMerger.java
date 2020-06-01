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

package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.function.Function;

/**
 * Merges sorted by keys streams and streams its sorted union. It is simple
 * {@link AbstractStreamReducer} which do nothing with data and only streams it
 * sorted by keys.
 *
 * @param <K> type of key for mapping
 * @param <T> type of output data
 */
public final class StreamMerger<K, T> extends AbstractStreamReducer<K, T, Void> {

	private final Function<T, K> keyFunction;
	private final Reducer<K, T, T, Void> reducer;

	// region creators
	private StreamMerger(@NotNull Function<T, K> keyFunction, @NotNull Comparator<K> keyComparator,
			boolean distinct) {
		super(keyComparator);
		this.keyFunction = keyFunction;
		this.reducer = distinct ? StreamReducers.mergeDistinctReducer() : StreamReducers.mergeSortReducer();
	}

	/**
	 * Returns new instance of StreamMerger
	 *
	 * @param keyComparator comparator for compare keys
	 * @param keyFunction   function for counting key
	 * @param distinct      if it is true it means that in result will be not objects with same key
	 * @param <K>           type of key for mapping
	 * @param <T>           type of output data
	 */
	public static <K, T> StreamMerger<K, T> create(Function<T, K> keyFunction,
			Comparator<K> keyComparator,
			boolean distinct) {
		return new StreamMerger<>(keyFunction, keyComparator, distinct);
	}

	@Override
	public StreamMerger<K, T> withBufferSize(int bufferSize) {
		return (StreamMerger<K, T>) super.withBufferSize(bufferSize);
	}
	// endregion

	/**
	 * Adds new consumer to  StreamMerger
	 *
	 * @return this consumer
	 */
	public StreamConsumer<T> newInput() {
		return newInput(keyFunction, reducer);
	}

}
