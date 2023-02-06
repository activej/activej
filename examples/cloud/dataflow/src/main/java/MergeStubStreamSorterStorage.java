import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.StreamSorterStorageFactory;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.processor.transformer.sort.IStreamSorterStorage;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.reactor.Reactive.checkInReactorThread;

public class MergeStubStreamSorterStorage<T> extends ImplicitlyReactive
		implements IStreamSorterStorage<T> {

	public static final StreamSorterStorageFactory FACTORY_STUB = new StreamSorterStorageFactory() {
		@Override
		public <C> IStreamSorterStorage<C> create(StreamSchema<C> streamSchema, Task context, Promise<Void> taskExecuted) {
			return new MergeStubStreamSorterStorage<>();
		}
	};

	private final Map<Integer, List<T>> storage = new HashMap<>();
	private int partition;

	private MergeStubStreamSorterStorage() {
	}

	@Override
	public Promise<Integer> newPartitionId() {
		checkInReactorThread(this);
		int newPartition = partition++;
		return Promise.of(newPartition);
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		checkInReactorThread(this);
		List<T> list = new ArrayList<>();
		storage.put(partition, list);
		StreamConsumer<T> consumer = ToListStreamConsumer.create(list);
		return Promise.of(consumer);
	}

	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		checkInReactorThread(this);
		List<T> iterable = storage.get(partition);
		StreamSupplier<T> supplier = StreamSuppliers.ofIterable(iterable);
		return Promise.of(supplier);
	}

	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		checkInReactorThread(this);
		for (Integer partition : partitionsToDelete) {
			storage.remove(partition);
		}
		return Promise.complete();
	}
}
