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

import io.activej.dataflow.dataset.impl.*;
import io.activej.dataflow.graph.Partition;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public final class Datasets {

	public static <K, T> SortedDataset<K, T> castToSorted(Dataset<T> dataset, Class<K> keyType,
			Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return new DatasetAlreadySorted<>(dataset, keyComparator, keyType, keyFunction);
	}

	public static <K, T> SortedDataset<K, T> castToSorted(LocallySortedDataset<K, T> dataset) {
		return castToSorted(dataset, dataset.keyType(), dataset.keyFunction(), dataset.keyComparator());
	}

	public static <K, L, R, V> SortedDataset<K, V> join(SortedDataset<K, L> left, SortedDataset<K, R> right,
			Joiner<K, L, R, V> joiner,
			Class<V> resultType, Function<V, K> keyFunction) {
		return new DatasetJoin<>(left, right, joiner, resultType, keyFunction);
	}

	public static <I, O> Dataset<O> map(Dataset<I> dataset, Function<I, O> mapper, Class<O> resultType) {
		return new DatasetMap<>(dataset, mapper, resultType);
	}

	public static <T> Dataset<T> map(Dataset<T> dataset, UnaryOperator<T> mapper) {
		return map(dataset, mapper, dataset.valueType());
	}

	public static <T> Dataset<T> filter(Dataset<T> dataset, Predicate<T> predicate) {
		return new DatasetFilter<>(dataset, predicate, dataset.valueType());
	}

	public static <K, I> LocallySortedDataset<K, I> localSort(Dataset<I> dataset, Class<K> keyType,
			Function<I, K> keyFunction, Comparator<K> keyComparator, int sortBufferSize) {
		return new DatasetLocalSort<>(dataset, keyType, keyFunction, keyComparator, sortBufferSize);
	}

	public static <K, I> LocallySortedDataset<K, I> localSort(Dataset<I> dataset, Class<K> keyType,
			Function<I, K> keyFunction, Comparator<K> keyComparator) {
		return localSort(dataset, keyType, keyFunction, keyComparator, 1_000_000);
	}

	public static <K, I, O> LocallySortedDataset<K, O> localReduce(LocallySortedDataset<K, I> stream,
			Reducer<K, I, O, ?> reducer,
			Class<O> resultType,
			Function<O, K> resultKeyFunction) {
		return new DatasetLocalSortReduce<>(stream, reducer, resultType, resultKeyFunction);
	}

	public static <T, K> Dataset<T> repartition(Dataset<T> dataset, Function<T, K> keyFunction, List<Partition> partitions) {
		return new DatasetRepartition<>(dataset, keyFunction, partitions);
	}

	public static <T, K> Dataset<T> repartition(Dataset<T> dataset, Function<T, K> keyFunction) {
		return new DatasetRepartition<>(dataset, keyFunction, null);
	}

	public static <K, I, O> Dataset<O> repartitionReduce(LocallySortedDataset<K, I> dataset,
			Reducer<K, I, O, ?> reducer,
			Class<O> resultType) {
		return new DatasetRepartitionReduce<>(dataset, reducer, resultType);
	}

	public static <K, I, O> Dataset<O> repartitionReduce(LocallySortedDataset<K, I> dataset,
			Reducer<K, I, O, ?> reducer,
			Class<O> resultType, List<Partition> partitions) {
		return new DatasetRepartitionReduce<>(dataset, reducer, resultType, partitions);
	}

	public static <K, T> SortedDataset<K, T> repartitionSort(LocallySortedDataset<K, T> dataset) {
		return new DatasetRepartitionAndSort<>(dataset);
	}

	public static <K, T> SortedDataset<K, T> repartitionSort(LocallySortedDataset<K, T> dataset,
			List<Partition> partitions) {
		return new DatasetRepartitionAndSort<>(dataset, partitions);
	}

	public static <K, I, O, A> Dataset<O> sortReduceRepartitionReduce(Dataset<I> dataset,
			ReducerToResult<K, I, O, A> reducer,
			Class<K> keyType,
			Function<I, K> inputKeyFunction,
			Comparator<K> keyComparator,
			Class<A> accumulatorType,
			Function<A, K> accumulatorKeyFunction,
			Class<O> outputType,
			int sortBufferSize) {
		LocallySortedDataset<K, I> partiallySorted = localSort(dataset, keyType, inputKeyFunction, keyComparator, sortBufferSize);
		LocallySortedDataset<K, A> partiallyReduced = localReduce(partiallySorted, reducer.inputToAccumulator(),
				accumulatorType, accumulatorKeyFunction);
		return repartitionReduce(partiallyReduced, reducer.accumulatorToOutput(), outputType);
	}

	public static <K, I, O, A> Dataset<O> sortReduceRepartitionReduce(Dataset<I> dataset,
			ReducerToResult<K, I, O, A> reducer,
			Class<K> keyType,
			Function<I, K> inputKeyFunction,
			Comparator<K> keyComparator,
			Class<A> accumulatorType,
			Function<A, K> accumulatorKeyFunction,
			Class<O> outputType) {
		return sortReduceRepartitionReduce(dataset, reducer, keyType, inputKeyFunction, keyComparator, accumulatorType, accumulatorKeyFunction, outputType, 1_000_000);
	}

	public static <K, I, A> Dataset<A> sortReduceRepartitionReduce(Dataset<I> dataset,
			ReducerToResult<K, I, A, A> reducer,
			Class<K> keyType,
			Function<I, K> inputKeyFunction,
			Comparator<K> keyComparator,
			Class<A> accumulatorType,
			Function<A, K> accumulatorKeyFunction) {
		return sortReduceRepartitionReduce(dataset, reducer,
				keyType, inputKeyFunction, keyComparator,
				accumulatorType, accumulatorKeyFunction, accumulatorType
		);
	}

	public static <K, T> Dataset<T> sortReduceRepartitionReduce(Dataset<T> dataset,
			ReducerToResult<K, T, T, T> reducer,
			Class<K> keyType, Function<T, K> keyFunction,
			Comparator<K> keyComparator) {
		return sortReduceRepartitionReduce(dataset, reducer,
				keyType, keyFunction, keyComparator,
				dataset.valueType(), keyFunction, dataset.valueType()
		);
	}

	public static <K, I, O, A> Dataset<O> splitSortReduceRepartitionReduce(Dataset<I> dataset,
			ReducerToResult<K, I, O, A> reducer,
			Function<I, K> inputKeyFunction,
			Comparator<K> keyComparator,
			Class<A> accumulatorType,
			Function<A, K> accumulatorKeyFunction,
			Class<O> outputType,
			int sortBufferSize) {
		return new DatasetSplitSortReduceRepartitionReduce<>(dataset, inputKeyFunction, accumulatorKeyFunction, keyComparator,
				reducer, outputType, accumulatorType, sortBufferSize);

	}

	public static <K, I, A> Dataset<A> splitSortReduceRepartitionReduce(Dataset<I> dataset,
			ReducerToResult<K, I, A, A> reducer,
			Function<I, K> inputKeyFunction,
			Comparator<K> keyComparator,
			Class<A> accumulatorType,
			Function<A, K> accumulatorKeyFunction) {
		return splitSortReduceRepartitionReduce(dataset, reducer,
				inputKeyFunction, keyComparator,
				accumulatorType, accumulatorKeyFunction, accumulatorType, 1_000_000
		);
	}

	public static <K, T> Dataset<T> splitSortReduceRepartitionReduce(Dataset<T> dataset,
			ReducerToResult<K, T, T, T> reducer,
			Function<T, K> keyFunction,
			Comparator<K> keyComparator) {
		return splitSortReduceRepartitionReduce(dataset, reducer,
				keyFunction, keyComparator,
				dataset.valueType(), keyFunction, dataset.valueType(), 1_000_000
		);
	}

	public static <T> Dataset<T> datasetOfId(String dataId, Class<T> resultType) {
		return new DatasetSupplierOfId<>(dataId, resultType, null);
	}

	public static <T> Dataset<T> datasetOfId(String dataId, Class<T> resultType, List<Partition> partitions) {
		return new DatasetSupplierOfId<>(dataId, resultType, partitions);
	}

	public static <K, T> SortedDataset<K, T> sortedDatasetOfId(String dataId, Class<T> resultType, Class<K> keyType,
			Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return castToSorted(datasetOfId(dataId, resultType), keyType, keyFunction, keyComparator);
	}

	public static <T> DatasetConsumerOfId<T> consumerOfId(Dataset<T> input, String listId) {
		return new DatasetConsumerOfId<>(input, listId);
	}

	public static <T> Dataset<T> empty(Class<T> resultType) {
		return new DatasetEmpty<>(resultType);
	}

	public static <T> Dataset<T> union(Dataset<T> left, Dataset<T> right) {
		return new DatasetUnion<>(left, right);
	}
}
