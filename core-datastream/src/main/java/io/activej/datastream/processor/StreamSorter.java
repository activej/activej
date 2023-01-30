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

import io.activej.async.AsyncAccumulator;
import io.activej.common.builder.AbstractBuilder;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.activej.datastream.processor.StreamReducers.deduplicateReducer;
import static io.activej.datastream.processor.StreamReducers.mergeReducer;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Represent {@link StreamTransformer} which receives data and saves it in
 * collection, when it receives end of stream it sorts it and streams to destination.
 *
 * @param <K> type of keys
 * @param <T> type of objects
 */
public final class StreamSorter<K, T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	private static final Logger logger = getLogger(StreamSorter.class);
	private final AsyncAccumulator<? extends List<Integer>> temporaryStreamsAccumulator;
	private final IStreamSorterStorage<T> storage;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final Comparator<T> itemComparator;
	private final boolean distinct;
	private final int itemsInMemory;

	private final Input input;
	private final StreamSupplier<T> output;

	private Executor sortingExecutor = Runnable::run;

	private StreamSorter(IStreamSorterStorage<T> storage,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate,
			int itemsInMemory) {
		this.storage = storage;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.itemComparator = (item1, item2) -> {
			K key1 = keyFunction.apply(item1);
			K key2 = keyFunction.apply(item2);
			return keyComparator.compare(key1, key2);
		};
		this.distinct = deduplicate;
		this.itemsInMemory = itemsInMemory;

		List<Integer> partitionIds = new ArrayList<>();
		this.input = new Input(partitionIds);
		this.output = StreamSupplier.ofPromise(
				(this.temporaryStreamsAccumulator = AsyncAccumulator.create(partitionIds))
						.get()
						.then(streamIds -> {
							ArrayList<T> sortedList = input.list;
							input.list = null;
							return Promise.ofBlocking(sortingExecutor, () -> sortedList.sort(itemComparator))
									.map($ -> {
										StreamSupplier<T> listSupplier = StreamSupplier.ofIterator(deduplicate ?
												new DistinctIterator<>(sortedList, keyFunction, keyComparator) :
												sortedList.iterator());
										logger.info("Items in memory: {}, files: {}", sortedList.size(), streamIds.size());
										if (streamIds.isEmpty()) {
											return listSupplier;
										}
										StreamReducer<K, T, Void> streamMerger = StreamReducer.create(keyComparator);
										listSupplier.streamTo(streamMerger.newInput(keyFunction, deduplicate ? deduplicateReducer() : mergeReducer()));
										for (Integer streamId : streamIds) {
											StreamSupplier.ofPromise(storage.read(streamId))
													.streamTo(streamMerger.newInput(keyFunction, deduplicate ? deduplicateReducer() : mergeReducer()));
										}
										return streamMerger.getOutput();
									});
						}));
	}

	/**
	 * Creates a new instance of StreamSorter
	 *
	 * @param storage           storage for storing elements which were not placed
	 *                          to RAM
	 * @param keyFunction       function for searching key
	 * @param keyComparator     comparator for comparing key
	 * @param distinct          if it is true it means that in result will be
	 *                          not objects with same key
	 * @param itemsInMemorySize size of elements which can be saved in RAM
	 *                          before sorting
	 */
	public static <K, T> StreamSorter<K, T> create(IStreamSorterStorage<T> storage,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct,
			int itemsInMemorySize) {
		return StreamSorter.builder(storage, keyFunction, keyComparator, distinct, itemsInMemorySize).build();
	}

	/**
	 * Creates a builder of StreamSorter
	 *
	 * @param storage           storage for storing elements which were not placed
	 *                          to RAM
	 * @param keyFunction       function for searching key
	 * @param keyComparator     comparator for comparing key
	 * @param distinct          if it is true it means that in result will be
	 *                          not objects with same key
	 * @param itemsInMemorySize size of elements which can be saved in RAM
	 *                          before sorting
	 */
	public static <K, T> StreamSorter<K, T>.Builder builder(IStreamSorterStorage<T> storage,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct,
			int itemsInMemorySize) {
		return new StreamSorter<>(storage, keyFunction, keyComparator, distinct, itemsInMemorySize).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, StreamSorter<K, T>>{
		private Builder() {}

		public Builder withSortingExecutor(Executor executor) {
			checkNotBuilt(this);
			sortingExecutor = executor;
			return this;
		}

		@Override
		protected StreamSorter<K, T> doBuild() {
			return StreamSorter.this;
		}
	}

	public static final class DistinctIterator<K, T> implements Iterator<T> {
		private final ArrayList<T> sortedList;
		private final Function<T, K> keyFunction;
		private final Comparator<K> keyComparator;
		int i = 0;

		private DistinctIterator(ArrayList<T> sortedList, Function<T, K> keyFunction, Comparator<K> keyComparator) {
			this.sortedList = sortedList;
			this.keyFunction = keyFunction;
			this.keyComparator = keyComparator;
		}

		@Override
		public boolean hasNext() {
			return i < sortedList.size();
		}

		@Override
		public T next() {
			T next = sortedList.get(i++);
			K nextKey = keyFunction.apply(next);
			while (i < sortedList.size()) {
				if (keyComparator.compare(nextKey, keyFunction.apply(sortedList.get(i))) == 0) {
					i++;
					continue;
				}
				break;
			}
			return next;
		}
	}

	public final class Input extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final List<Integer> partitionIds;

		private ArrayList<T> list = new ArrayList<>();
		private Promise<Void> cleanupPromise;

		private Input(List<Integer> partitionIds) {
			this.partitionIds = partitionIds;
		}

		@Override
		protected void onStarted() {
			resume(this);
		}

		@Override
		public void accept(T item) {
			list.add(item);
			if (list.size() < itemsInMemory) {
				return;
			}
			ArrayList<T> sortedList = this.list;
			list = new ArrayList<>(itemsInMemory);

			temporaryStreamsAccumulator.addPromise(
					Promise.ofBlocking(sortingExecutor, () -> sortedList.sort(itemComparator))
							.then($ -> {
								Iterator<T> iterator = distinct ?
										new DistinctIterator<>(sortedList, keyFunction, keyComparator) :
										sortedList.iterator();
								return storage.newPartitionId()
										.then(partitionId -> storage.write(partitionId)
												.then(consumer -> StreamSupplier.ofIterator(iterator).streamTo(consumer))
												.map($2 -> partitionId));
							})
							.whenResult(this::suspendOrResume)
							.whenException(this::closeEx), List::add);

			suspendOrResume();
		}

		private void suspendOrResume() {
			if (temporaryStreamsAccumulator.getActivePromises() > 2) {
				suspend();
			} else {
				resume(this);
			}
		}

		@Override
		protected void onEndOfStream() {
			temporaryStreamsAccumulator.run();
			output.getAcknowledgement()
					.then((ackRes, e) -> cleanup()
							.then(($, e1) -> Promise.of(ackRes, e)))
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onError(Exception e) {
			temporaryStreamsAccumulator.closeEx(e);
		}

		@Override
		protected void onCleanup() {
			list = null;
			cleanup();
		}

		private Promise<Void> cleanup() {
			if (cleanupPromise != null) return cleanupPromise;
			return cleanupPromise = partitionIds.isEmpty() ?
					Promise.complete() :
					storage.cleanup(partitionIds);
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

}
