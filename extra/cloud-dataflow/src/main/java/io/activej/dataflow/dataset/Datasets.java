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

package io.activej.dataflow.dataset;

import io.activej.common.ApplicationSettings;
import io.activej.common.annotation.StaticFactories;
import io.activej.dataflow.dataset.impl.*;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.datastream.processor.join.LeftJoiner;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.ReducerToResult;
import io.activej.datastream.processor.transformer.impl.Limiter;
import io.activej.datastream.processor.transformer.impl.Skip;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.activej.common.Checks.checkArgument;

@StaticFactories(Dataset.class)
public class Datasets {
	private static final int DEFAULT_MEMORY_SORT_BUFFER_SIZE = ApplicationSettings.getInt(Datasets.class, "memorySortBufferSize", 1_000_000);

	public static <K, T> SortedDataset<K, T> castToSorted(
		Dataset<T> dataset, Class<K> keyType, Function<T, K> keyFunction, Comparator<K> keyComparator
	) {
		return new AlreadySorted<>(dataset, keyComparator, keyType, keyFunction);
	}

	public static <K, T> SortedDataset<K, T> castToSorted(LocallySortedDataset<K, T> dataset) {
		return castToSorted(dataset, dataset.keyType(), dataset.keyFunction(), dataset.keyComparator());
	}

	public static <K, L, R, V> SortedDataset<K, V> join(
		SortedDataset<K, L> left, SortedDataset<K, R> right, LeftJoiner<K, L, R, V> leftJoiner,
		StreamSchema<V> resultStreamSchema, Function<V, K> keyFunction
	) {
		return new Join<>(left, right, leftJoiner, resultStreamSchema, keyFunction);
	}

	public static <I, O> Dataset<O> map(Dataset<I> dataset, Function<I, O> mapper, StreamSchema<O> resultStreamSchema) {
		return new Map<>(dataset, mapper, resultStreamSchema);
	}

	public static <T> Dataset<T> map(Dataset<T> dataset, UnaryOperator<T> mapper) {
		return map(dataset, mapper, dataset.streamSchema());
	}

	public static <T> Dataset<T> filter(Dataset<T> dataset, Predicate<T> predicate) {
		return new Filter<>(dataset, predicate);
	}

	public static <K, I> LocallySortedDataset<K, I> localSort(
		Dataset<I> dataset, Class<K> keyType, Function<I, K> keyFunction, Comparator<K> keyComparator,
		int sortBufferSize
	) {
		return new LocalSort<>(dataset, keyType, keyFunction, keyComparator, sortBufferSize);
	}

	public static <K, I> LocallySortedDataset<K, I> localSort(
		Dataset<I> dataset, Class<K> keyType, Function<I, K> keyFunction, Comparator<K> keyComparator
	) {
		return localSort(dataset, keyType, keyFunction, keyComparator, DEFAULT_MEMORY_SORT_BUFFER_SIZE);
	}

	public static <K, I, O> LocallySortedDataset<K, O> localReduce(
		LocallySortedDataset<K, I> stream, Reducer<K, I, O, ?> reducer, StreamSchema<O> resultStreamSchema,
		Function<O, K> resultKeyFunction
	) {
		return new LocalSortReduce<>(stream, reducer, resultStreamSchema, resultKeyFunction);
	}

	public static <T, K> Dataset<T> repartition(Dataset<T> dataset, Function<T, K> keyFunction, List<Partition> partitions) {
		return new Repartition<>(dataset, keyFunction, partitions);
	}

	public static <T, K> Dataset<T> repartition(Dataset<T> dataset, Function<T, K> keyFunction) {
		return new Repartition<>(dataset, keyFunction, null);
	}

	public static <K, I, O> Dataset<O> repartitionReduce(
		LocallySortedDataset<K, I> dataset, Reducer<K, I, O, ?> reducer, StreamSchema<O> resultStreamSchema
	) {
		return new RepartitionReduce<>(dataset, reducer, resultStreamSchema, null);
	}

	public static <K, I, O> Dataset<O> repartitionReduce(
		LocallySortedDataset<K, I> dataset, Reducer<K, I, O, ?> reducer, StreamSchema<O> resultStreamSchema,
		List<Partition> partitions
	) {
		return new RepartitionReduce<>(dataset, reducer, resultStreamSchema, partitions);
	}

	public static <K, T> SortedDataset<K, T> repartitionSort(LocallySortedDataset<K, T> dataset) {
		return new RepartitionAndSort<>(dataset, null);
	}

	public static <K, T> SortedDataset<K, T> repartitionSort(
		LocallySortedDataset<K, T> dataset, List<Partition> partitions
	) {
		return new RepartitionAndSort<>(dataset, partitions);
	}

	public static <K, I, O, A> Dataset<O> sortReduceRepartitionReduce(
		Dataset<I> dataset, ReducerToResult<K, I, O, A> reducer, Class<K> keyType, Function<I, K> inputKeyFunction,
		Comparator<K> keyComparator, StreamSchema<A> accumulatorStreamSchema, Function<A, K> accumulatorKeyFunction,
		StreamSchema<O> outputStreamSchema, int sortBufferSize
	) {
		LocallySortedDataset<K, I> partiallySorted = localSort(dataset, keyType, inputKeyFunction, keyComparator, sortBufferSize);
		LocallySortedDataset<K, A> partiallyReduced = localReduce(partiallySorted, reducer.inputToAccumulator(),
			accumulatorStreamSchema, accumulatorKeyFunction);
		return repartitionReduce(partiallyReduced, reducer.accumulatorToOutput(), outputStreamSchema);
	}

	public static <K, I, O, A> Dataset<O> sortReduceRepartitionReduce(
		Dataset<I> dataset, ReducerToResult<K, I, O, A> reducer, Class<K> keyType, Function<I, K> inputKeyFunction,
		Comparator<K> keyComparator, StreamSchema<A> accumulatorStreamSchema, Function<A, K> accumulatorKeyFunction,
		StreamSchema<O> outputStreamSchema
	) {
		return sortReduceRepartitionReduce(dataset, reducer, keyType, inputKeyFunction, keyComparator, accumulatorStreamSchema, accumulatorKeyFunction, outputStreamSchema, DEFAULT_MEMORY_SORT_BUFFER_SIZE);
	}

	public static <K, I, A> Dataset<A> sortReduceRepartitionReduce(
		Dataset<I> dataset, ReducerToResult<K, I, A, A> reducer, Class<K> keyType, Function<I, K> inputKeyFunction,
		Comparator<K> keyComparator, StreamSchema<A> accumulatorStreamSchema, Function<A, K> accumulatorKeyFunction
	) {
		return sortReduceRepartitionReduce(dataset, reducer,
			keyType, inputKeyFunction, keyComparator,
			accumulatorStreamSchema, accumulatorKeyFunction, accumulatorStreamSchema
		);
	}

	public static <K, T> Dataset<T> sortReduceRepartitionReduce(
		Dataset<T> dataset, ReducerToResult<K, T, T, T> reducer, Class<K> keyType, Function<T, K> keyFunction,
		Comparator<K> keyComparator
	) {
		return sortReduceRepartitionReduce(dataset, reducer,
			keyType, keyFunction, keyComparator,
			dataset.streamSchema(), keyFunction, dataset.streamSchema()
		);
	}

	public static <K, I, O, A> Dataset<O> splitSortReduceRepartitionReduce(
		Dataset<I> dataset, ReducerToResult<K, I, O, A> reducer, Function<I, K> inputKeyFunction,
		Comparator<K> keyComparator, StreamSchema<A> accumulatorStreamSchema, Function<A, K> accumulatorKeyFunction,
		StreamSchema<O> outputStreamSchema, int sortBufferSize
	) {
		return new SplitSortReduceRepartitionReduce<>(dataset, inputKeyFunction, accumulatorKeyFunction, keyComparator,
			reducer, outputStreamSchema, accumulatorStreamSchema, sortBufferSize);
	}

	public static <K, I, A> Dataset<A> splitSortReduceRepartitionReduce(
		Dataset<I> dataset, ReducerToResult<K, I, A, A> reducer, Function<I, K> inputKeyFunction,
		Comparator<K> keyComparator, StreamSchema<A> accumulatorStreamSchema, Function<A, K> accumulatorKeyFunction
	) {
		return splitSortReduceRepartitionReduce(dataset, reducer,
			inputKeyFunction, keyComparator,
			accumulatorStreamSchema, accumulatorKeyFunction, accumulatorStreamSchema, DEFAULT_MEMORY_SORT_BUFFER_SIZE
		);
	}

	public static <K, T> Dataset<T> splitSortReduceRepartitionReduce(
		Dataset<T> dataset, ReducerToResult<K, T, T, T> reducer, Function<T, K> keyFunction,
		Comparator<K> keyComparator
	) {
		return splitSortReduceRepartitionReduce(dataset, reducer,
			keyFunction, keyComparator,
			dataset.streamSchema(), keyFunction, dataset.streamSchema(), DEFAULT_MEMORY_SORT_BUFFER_SIZE
		);
	}

	public static <T> Dataset<T> datasetOfId(String dataId, StreamSchema<T> resultStreamSchema) {
		return new SupplierOfId<>(dataId, resultStreamSchema, null);
	}

	public static <T> Dataset<T> datasetOfId(String dataId, StreamSchema<T> resultStreamSchema, List<Partition> partitions) {
		return new SupplierOfId<>(dataId, resultStreamSchema, partitions);
	}

	public static <K, T> SortedDataset<K, T> sortedDatasetOfId(
		String dataId, StreamSchema<T> resultStreamSchema, Class<K> keyType, Function<T, K> keyFunction,
		Comparator<K> keyComparator
	) {
		return castToSorted(datasetOfId(dataId, resultStreamSchema), keyType, keyFunction, keyComparator);
	}

	public static <T> Dataset<T> consumerOfId(Dataset<T> input, String listId) {
		return new ConsumerOfId<>(input, listId);
	}

	public static <T> Dataset<T> empty(StreamSchema<T> resultStreamSchema, List<Partition> partitions) {
		return new Empty<>(resultStreamSchema, partitions);
	}

	public static <T> Dataset<T> empty(StreamSchema<T> resultStreamSchema) {
		return new Empty<>(resultStreamSchema, null);
	}

	public static <T> Dataset<T> unionAll(Dataset<T> left, Dataset<T> right) {
		return new UnionAll<>(left, right);
	}

	public static <K, T> SortedDataset<K, T> union(SortedDataset<K, T> left, SortedDataset<K, T> right) {
		return new Union<>(left, right, ThreadLocalRandom.current().nextInt());
	}

	public static <K, T> SortedDataset<K, T> offset(LocallySortedDataset<K, T> dataset, long offset) {
		return offsetLimit(dataset, offset, Limiter.NO_LIMIT);
	}

	public static <K, T> SortedDataset<K, T> limit(LocallySortedDataset<K, T> dataset, long limit) {
		return offsetLimit(dataset, Skip.NO_SKIP, limit);
	}

	public static <K, T> SortedDataset<K, T> offsetLimit(LocallySortedDataset<K, T> dataset, long offset, long limit) {
		checkArgument(offset >= Skip.NO_SKIP && limit >= Limiter.NO_LIMIT, "Negative offset or limit");

		return new SortedOffsetLimit<>(dataset, offset, limit, ThreadLocalRandom.current().nextInt());
	}

	public static <T, K> Dataset<T> offset(Dataset<T> dataset, Function<T, K> keyFunction, long offset) {
		return offsetLimit(dataset, keyFunction, offset, Limiter.NO_LIMIT);
	}

	public static <T, K> Dataset<T> limit(Dataset<T> dataset, Function<T, K> keyFunction, long limit) {
		return offsetLimit(dataset, keyFunction, Skip.NO_SKIP, limit);
	}

	public static <T, K> Dataset<T> offsetLimit(Dataset<T> dataset, Function<T, K> keyFunction, long offset, long limit) {
		checkArgument(offset >= Skip.NO_SKIP && limit >= Limiter.NO_LIMIT, "Negative offset or limit");

		return new OffsetLimit<>(dataset, keyFunction, offset, limit, ThreadLocalRandom.current().nextInt());
	}

	public static <T> Dataset<T> localLimit(Dataset<T> dataset, long limit) {
		checkArgument(limit >= Limiter.NO_LIMIT, "Negative limit");

		return new LocalLimit<>(dataset, limit);
	}
}
