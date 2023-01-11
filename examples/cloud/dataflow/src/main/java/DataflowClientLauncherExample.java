import dto.CreateStringCountFunction;
import dto.ExtractStringFunction;
import dto.StringCount;
import dto.StringCountReducer;
import io.activej.config.Config;
import io.activej.dataflow.collector.AsyncCollector;
import io.activej.dataflow.collector.Collector_Merge;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.node.Node_Sort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.inject.annotation.Inject;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.launchers.dataflow.DataflowClientLauncher;
import io.activej.reactor.Reactor;

import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.graph.StreamSchemas.simple;
import static java.util.Comparator.naturalOrder;

/**
 * This launcher posts a simple Map-Reduce task to a cluster of Dataflow nodes.
 * You must specify nodes' addresses as program arguments.
 * <p>
 * These servers must provide a dataset of strings with "items" as their ids.
 */
//[START REGION_1]
public final class DataflowClientLauncherExample extends DataflowClientLauncher {
	private static final String DEFAULT_PARTITION = "127.0.0.1:9000";

	@Inject
	DataflowGraph graph;

	@Inject
	Reactor reactor;

	@Override
	protected Module getOverrideModule() {
		return ModuleBuilder.create()
				.install(new DataflowSerializersModule())

				.bind(StreamSorterStorageFactory.class).toInstance(StreamSorterStorage_MergeStub.FACTORY_STUB)

				.bind(Config.class).toInstance(
						Config.create()
								.with("dataflow.partitions", args.length == 0 ? DEFAULT_PARTITION : String.join(",", args)))
				.build();
	}
	//[END REGION_1]

	//[START REGION_2]
	@Override
	protected void run() throws InterruptedException {
		reactor.execute(() -> {
			StringCountReducer reducer = new StringCountReducer();
			ExtractStringFunction keyFunction = new ExtractStringFunction();

			Dataset<String> items = datasetOfId("items", simple(String.class));

			Dataset<StringCount> mappedItems = map(items, new CreateStringCountFunction(), simple(StringCount.class));

			LocallySortedDataset<String, StringCount> locallySorted = localSort(mappedItems, String.class, keyFunction, naturalOrder());

			LocallySortedDataset<String, StringCount> locallyReduced = localReduce(locallySorted, reducer.inputToAccumulator(), simple(StringCount.class), keyFunction);

			Dataset<StringCount> reducedItems = repartitionReduce(locallyReduced, reducer.accumulatorToOutput(), simple(StringCount.class));

			AsyncCollector<StringCount> collector = Collector_Merge.create(reducedItems, client, keyFunction, naturalOrder(), false);

			StreamSupplier<StringCount> resultSupplier = collector.compile(graph);

			StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();

			System.out.println("\n *** Dataset graph:\n");
			System.out.println(reducedItems.toGraphViz());
			System.out.println("\n *** Compiled nodes graph:\n");
			System.out.println(graph.toGraphViz());

			graph.execute().both(resultSupplier.streamTo(resultConsumer))
					.whenException(Exception::printStackTrace)
					.whenResult(() -> {
						System.out.println("Top 100 words:");
						resultConsumer.getList().stream().limit(100).forEach(System.out::println);
					})
					.whenComplete(this::shutdown);
		});

		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new DataflowClientLauncherExample().launch(args);
	}
	//[END REGION_2]
}
