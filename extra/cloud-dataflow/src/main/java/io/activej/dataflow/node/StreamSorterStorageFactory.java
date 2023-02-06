package io.activej.dataflow.node;

import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.datastream.processor.transformer.sort.IStreamSorterStorage;
import io.activej.promise.Promise;

public interface StreamSorterStorageFactory {
	<T> IStreamSorterStorage<T> create(StreamSchema<T> streamSchema, Task context, Promise<Void> taskExecuted);

	default <T> Promise<Void> cleanup(IStreamSorterStorage<T> storage) {
		return Promise.complete();
	}
}
