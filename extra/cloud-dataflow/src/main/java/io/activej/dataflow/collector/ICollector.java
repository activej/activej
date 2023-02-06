package io.activej.dataflow.collector;

import io.activej.common.annotation.ComponentInterface;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.datastream.supplier.StreamSupplier;

@ComponentInterface
public interface ICollector<T> {
	StreamSupplier<T> compile(DataflowGraph graph);
}
