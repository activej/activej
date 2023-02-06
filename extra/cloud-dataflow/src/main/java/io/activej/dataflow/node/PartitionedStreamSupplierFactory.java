package io.activej.dataflow.node;

import io.activej.datastream.supplier.StreamSupplier;

public interface PartitionedStreamSupplierFactory<T> {
	StreamSupplier<T> get(int partitionIndex, int maxPartitions);
}
