package io.activej.dataflow.stats;

import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamConsumerTransformer;
import io.activej.datastream.processor.transformer.StreamSupplierTransformer;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.supplier.StreamSupplier;

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
