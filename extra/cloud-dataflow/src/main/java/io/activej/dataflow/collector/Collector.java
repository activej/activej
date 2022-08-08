package io.activej.dataflow.collector;

import io.activej.dataflow.graph.DataflowGraph;
import io.activej.datastream.StreamSupplier;

public interface Collector<T> {
	StreamSupplier<T> compile(DataflowGraph graph);
}
