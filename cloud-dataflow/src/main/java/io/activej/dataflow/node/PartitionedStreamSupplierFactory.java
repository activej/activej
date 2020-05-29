package io.activej.dataflow.node;

import io.activej.datastream.StreamSupplier;

public interface PartitionedStreamSupplierFactory<T> {
	StreamSupplier<T> get(int partitionIndex, int maxPartitions);
}
