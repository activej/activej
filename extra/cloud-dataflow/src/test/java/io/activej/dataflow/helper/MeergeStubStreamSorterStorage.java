package io.activej.dataflow.helper;

import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.AsyncStreamSorterStorage;
import io.activej.promise.Promise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MeergeStubStreamSorterStorage<T> implements AsyncStreamSorterStorage<T> {

	public static final StreamSorterStorageFactory FACTORY_STUB = new StreamSorterStorageFactory() {
		@Override
		public <C> AsyncStreamSorterStorage<C> create(StreamSchema<C> streamSchema, Task context, Promise<Void> taskExecuted) {
			return new MeergeStubStreamSorterStorage<>();
		}
	};

	private final Map<Integer, List<T>> storage = new HashMap<>();
	private int partition;

	private MeergeStubStreamSorterStorage() {
	}

	@Override
	public Promise<Integer> newPartitionId() {
		int newPartition = partition++;
		return Promise.of(newPartition);
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		List<T> list = new ArrayList<>();
		storage.put(partition, list);
		StreamConsumerToList<T> consumer = StreamConsumerToList.create(list);
		return Promise.of(consumer);
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		List<T> iterable = storage.get(partition);
		StreamSupplier<T> supplier = StreamSupplier.ofIterable(iterable);
		return Promise.of(supplier);
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		for (Integer partition : partitionsToDelete) {
			storage.remove(partition);
		}
		return Promise.complete();
	}
}
