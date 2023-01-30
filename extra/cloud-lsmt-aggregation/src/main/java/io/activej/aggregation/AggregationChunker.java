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

package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationStructure;
import io.activej.aggregation.util.PartitionPredicate;
import io.activej.async.AsyncAccumulator;
import io.activej.codegen.DefiningClassLoader;
import io.activej.datastream.ForwardingStreamConsumer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumer_Switcher;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;

import java.util.ArrayList;
import java.util.List;

public final class AggregationChunker<C, T> extends ForwardingStreamConsumer<T> {
	private final StreamConsumer_Switcher<T> switcher;
	private final SettablePromise<List<AggregationChunk>> result = new SettablePromise<>();

	private final AggregationStructure aggregation;
	private final List<String> fields;
	private final Class<T> recordClass;
	private final PartitionPredicate<T> partitionPredicate;
	private final IAggregationChunkStorage<C> storage;
	private final AsyncAccumulator<List<AggregationChunk>> chunksAccumulator;
	private final DefiningClassLoader classLoader;

	private final int chunkSize;

	private AggregationChunker(StreamConsumer_Switcher<T> switcher,
			AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
			IAggregationChunkStorage<C> storage,
			DefiningClassLoader classLoader,
			int chunkSize) {
		super(switcher);
		this.switcher = switcher;
		this.aggregation = aggregation;
		this.fields = fields;
		this.recordClass = recordClass;
		this.partitionPredicate = partitionPredicate;
		this.storage = storage;
		this.classLoader = classLoader;
		this.chunkSize = chunkSize;
		(this.chunksAccumulator = AsyncAccumulator.create(new ArrayList<>()))
				.run(getAcknowledgement())
				.whenComplete(result::trySet);
	}

	public static <C, T> AggregationChunker<C, T> create(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, PartitionPredicate<T> partitionPredicate,
			IAggregationChunkStorage<C> storage,
			DefiningClassLoader classLoader,
			int chunkSize) {

		StreamConsumer_Switcher<T> switcher = StreamConsumer_Switcher.create();
		AggregationChunker<C, T> chunker = new AggregationChunker<>(switcher, aggregation, fields, recordClass, partitionPredicate, storage, classLoader, chunkSize);
		chunker.startNewChunk();
		return chunker;
	}

	public Promise<List<AggregationChunk>> getResult() {
		return result;
	}

	public class ChunkWriter extends ForwardingStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final SettablePromise<AggregationChunk> result = new SettablePromise<>();
		private final int chunkSize;
		private final PartitionPredicate<T> partitionPredicate;
		private StreamDataAcceptor<T> dataAcceptor;

		private T first;
		private T last;
		private int count;

		public ChunkWriter(StreamConsumer<T> actualConsumer,
				C chunkId, int chunkSize, PartitionPredicate<T> partitionPredicate) {
			super(actualConsumer);
			this.chunkSize = chunkSize;
			this.partitionPredicate = partitionPredicate;
			actualConsumer.getAcknowledgement()
					.map($ -> count == 0 ?
							null :
							AggregationChunk.create(chunkId,
									fields,
									PrimaryKey.ofObject(first, aggregation.getKeys()),
									PrimaryKey.ofObject(last, aggregation.getKeys()),
									count))
					.whenComplete(result::trySet);
		}

		@Override
		public StreamDataAcceptor<T> getDataAcceptor() {
			this.dataAcceptor = super.getDataAcceptor();
			return this.dataAcceptor != null ? this : null;
		}

		@Override
		public void accept(T item) {
			if (first == null) {
				first = item;
			}
			last = item;
			dataAcceptor.accept(item);
			if (++count == chunkSize || (partitionPredicate != null && !partitionPredicate.isSamePartition(last, item))) {
				startNewChunk();
			}
		}

		public Promise<AggregationChunk> getResult() {
			return result;
		}
	}

	private void startNewChunk() {
		switcher.switchTo(StreamConsumer.ofPromise(
				storage.createId()
						.then(chunkId -> storage.write(aggregation, fields, recordClass, chunkId, classLoader)
								.map(streamConsumer -> new ChunkWriter(streamConsumer, chunkId, chunkSize, partitionPredicate))
								.whenResult(chunkWriter -> chunksAccumulator.addPromise(
										chunkWriter.getResult(),
										(chunks, newChunk) -> {
											if (newChunk != null && newChunk.getCount() != 0) {
												chunks.add(newChunk);
											}
										})))));
	}

}
