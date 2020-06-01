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

import java.util.Comparator;
import java.util.function.Function;

/**
 * Perform aggregative functions on the elements sorted by keys from input streams. Searches key of item
 * with key function, selects elements with some key, reductions it and streams result sorted by key.
 * It is {@link AbstractStreamReducer}.
 *
 * @param <K> type of key of element
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
public final class StreamReducer<K, O, A> extends AbstractStreamReducer<K, O, A> {
	// region creators
	private StreamReducer(Comparator<K> keyComparator) {
		super(keyComparator);
	}

	/**
	 * Creates a new instance of StreamReducer
	 *
	 * @param keyComparator comparator for compare keys
	 */
	public static <K, O, A> StreamReducer<K, O, A> create(Comparator<K> keyComparator) {
		return new StreamReducer<>(keyComparator);
	}

	@Override
	public StreamReducer<K, O, A> withBufferSize(int bufferSize) {
		return (StreamReducer<K, O, A>) super.withBufferSize(bufferSize);
	}
	// endregion

	/**
	 * Creates a new input stream for this reducer
	 *
	 * @param keyFunction function for counting key
	 * @param reducer     reducer witch will performs actions with its stream
	 * @param <I>         type of input data
	 * @return new consumer
	 */
	@Override
	public <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, Reducer<K, I, O, A> reducer) {
		return super.newInput(keyFunction, reducer);
	}
}
