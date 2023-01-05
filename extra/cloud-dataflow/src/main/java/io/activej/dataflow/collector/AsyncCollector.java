package io.activej.dataflow.collector;

import io.activej.dataflow.graph.DataflowGraph;
import io.activej.datastream.StreamSupplier;

public interface AsyncCollector<T> {
	StreamSupplier<T> compile(DataflowGraph graph);
}
