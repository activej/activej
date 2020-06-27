import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamSorterStorage;
import io.activej.promise.Promise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamMergeSorterStorageStub<T> implements StreamSorterStorage<T> {

	public static final StreamSorterStorageFactory FACTORY_STUB = new StreamSorterStorageFactory() {
		@Override
		public <C> StreamSorterStorage<C> create(Class<C> type, Task context, Promise<Void> taskExecuted) {
			return new StreamMergeSorterStorageStub<>();
		}
	};

	private final Map<Integer, List<T>> storage = new HashMap<>();
	private int partition;

	private StreamMergeSorterStorageStub() {
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
