package io.activej.dataflow.node;

import io.activej.common.annotation.StaticFactories;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.node.impl.*;
import io.activej.datastream.processor.StreamLeftJoin;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

@StaticFactories(Node.class)
public class Nodes {
	public static Node consumerOfId(int index, String id, int partitionIndex, int maxPartitions, StreamId input) {
		return new ConsumerOfId(index, id, partitionIndex, maxPartitions, input);
	}

	public static <T> Node download(int index, StreamSchema<T> streamSchema, InetSocketAddress address, StreamId streamId) {
		return new Download<>(index, streamSchema, address, streamId, new StreamId());
	}

	public static <T> Node filter(int index, Predicate<T> predicate, StreamId input) {
		return new Filter<>(index, predicate, input, new StreamId());
	}

	public static <K, L, R, V> Node join(int index, StreamId left, StreamId right,
			Comparator<K> keyComparator, Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
			StreamLeftJoin.LeftJoiner<K, L, R, V> leftJoiner) {
		return new Join<>(index, left, right, new StreamId(), keyComparator, leftKeyFunction, rightKeyFunction, leftJoiner);
	}

	public static <I, O> Node map(int index, Function<I, O> function, StreamId input) {
		return new Map<>(index, function, input, new StreamId());
	}

	public static Node offsetLimit(int index, long offset, long limit, StreamId input) {
		return new OffsetLimit(index, offset, limit, input, new StreamId());
	}

	public static <K, T> Node sort(int index, StreamSchema<T> streamSchema, Function<T, K> keyFunction, Comparator<K> keyComparator,
			boolean deduplicate, int itemsInMemorySize, StreamId input) {
		return new Sort<>(index, streamSchema, keyFunction, keyComparator, deduplicate, itemsInMemorySize, input, new StreamId());
	}

	public static Node emptySupplier(int index) {
		return new EmptySupplier(index, new StreamId());
	}

	public static Node supplierOfId(int index, String id, int partitionIndex, int maxPartitions) {
		return new SupplierOfId(index, id, partitionIndex, maxPartitions, new StreamId());
	}

	public static Node union(int index, List<StreamId> inputs) {
		return new Union(index, inputs, new StreamId());
	}

	public static <T> Node upload(int index, StreamSchema<T> streamSchema, StreamId streamId) {
		return new Upload<>(index, streamSchema, streamId);
	}
}
