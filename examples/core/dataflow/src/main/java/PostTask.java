import dto.CreateStringCountFunction;
import dto.ExtractStringFunction;
import dto.StringCount;
import dto.StringCountReducer;
import io.activej.config.Config;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.launchers.dataflow.DataflowClientLauncher;

import java.util.Comparator;

import static io.activej.codec.StructuredCodec.ofObject;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.inject.CodecsModule.codec;

/**
 * This launcher posts a simple Map-Reduce task to a cluster of Dataflow nodes
 * addresses of which should be specified as program arguments.
 * <p>
 * These servers must provide a dataset of strings with "items" as its id.
 */
public final class PostTask extends DataflowClientLauncher {
	public static final String PROPERTIES_FILE = "dataflow-client.properties";

	@Inject
	DataflowClient client;

	@Inject
	DataflowGraph graph;

	@Inject
	Eventloop eventloop;

	@Override
	protected Module getOverrideModule() {
		return ModuleBuilder.create()
				.bind(codec(CreateStringCountFunction.class)).toInstance(ofObject(CreateStringCountFunction::new))
				.bind(codec(ExtractStringFunction.class)).toInstance(ofObject(ExtractStringFunction::new))
				.bind(codec(StringCountReducer.class)).toInstance(ofObject(StringCountReducer::new))

				.bind(StreamSorterStorageFactory.class).toInstance(StreamMergeSorterStorageStub.FACTORY_STUB)

				.bind(Config.class).toInstance(Config.create()
						.with("dataflow.secondaryBufferPath", Util.createTempDir("dataflow-client-secondary-storage"))
						.with("dataflow.partitions", String.join(",", args)))
				.build();
	}

	@Override
	protected void run() throws Exception {
		eventloop.execute(() -> {
			StringCountReducer reducer = new StringCountReducer();
			ExtractStringFunction keyFunction = new ExtractStringFunction();

			Dataset<String> items = datasetOfId("items", String.class);

			Dataset<StringCount> mappedItems = map(items, new CreateStringCountFunction(), StringCount.class);

			LocallySortedDataset<String, StringCount> locallySorted = localSort(mappedItems, String.class, keyFunction, Comparator.naturalOrder());

			LocallySortedDataset<String, StringCount> locallyReduced = localReduce(locallySorted, reducer.inputToAccumulator(), StringCount.class, keyFunction);

			Dataset<StringCount> reducedItems = repartitionReduce(locallyReduced, reducer.accumulatorToOutput(), StringCount.class);

			Collector<StringCount> collector = new Collector<>(reducedItems, client);

			StreamSupplier<StringCount> resultSupplier = collector.compile(graph);

			StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();

			System.out.println("\n *** Dataset graph:\n");
			System.out.println(reducedItems.toGraphViz());
			System.out.println("\n *** Compiled nodes graph:\n");
			System.out.println(graph.toGraphViz());

			graph.execute().both(resultSupplier.streamTo(resultConsumer))
					.whenComplete(() -> {
						System.out.println("Top 100 words:");
						resultConsumer.getList().stream().limit(100).forEach(System.out::println);
						shutdown();
					});
		});

		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new PostTask().launch(args);
	}
}
