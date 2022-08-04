package io.activej.dataflow.calcite.aggregation;

import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.DatasetUtils;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class DatasetCalciteAggregate extends LocallySortedDataset<RecordScheme, Record> {
	private final LocallySortedDataset<RecordScheme, Record> input;
	private final RecordReducer reducer;

	private final int sharderNonce = ThreadLocalRandom.current().nextInt();

	private DatasetCalciteAggregate(LocallySortedDataset<RecordScheme, Record> input, RecordReducer reducer) {
		super(input.valueType(), input.keyComparator(), input.keyType(), input.keyFunction());
		this.input = input;
		this.reducer = reducer;
	}

	public static DatasetCalciteAggregate create(LocallySortedDataset<RecordScheme, Record> input, RecordReducer reducer) {
		return new DatasetCalciteAggregate(input, reducer);
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowContext next = context.withFixedNonce(sharderNonce);

		List<StreamId> streamIds = input.channels(next);

		DataflowGraph graph = next.getGraph();

		StreamId randomStreamId = streamIds.get(Math.abs(sharderNonce) % streamIds.size());
		Partition randomPartition = graph.getPartition(randomStreamId);

		List<StreamId> newStreamIds = DatasetUtils.repartitionAndReduce(next, streamIds, valueType(), input.keyFunction(), input.keyComparator(), reducer.getAccumulatorToOutput(), List.of(randomPartition));
		assert newStreamIds.size() == 1;

		return newStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(input);
	}
}
