package io.activej.dataflow.stats;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamConsumerTransformer;
import io.activej.datastream.processor.StreamSupplierTransformer;
import io.activej.datastream.stats.StreamStats;

public class NodeStreamStat<T> extends NodeStat implements StreamSupplierTransformer<T, StreamSupplier<T>>, StreamConsumerTransformer<T, StreamConsumer<T>> {
	private final StreamStats<T> streamStats;

	public NodeStreamStat(StreamStats<T> streamStats) {
		this.streamStats = streamStats;
	}

	@Override
	public StreamConsumer<T> transform(StreamConsumer<T> consumer) {
		return streamStats.transform(consumer);
	}

	@Override
	public StreamSupplier<T> transform(StreamSupplier<T> supplier) {
		return streamStats.transform(supplier);
	}

	@Override
	public String toString() {
		return "NodeStreamStats{}";
	}
}
