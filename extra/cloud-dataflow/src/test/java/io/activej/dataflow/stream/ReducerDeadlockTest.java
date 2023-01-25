package io.activej.dataflow.stream;

import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.datastream.StreamConsumer_ToList;
import io.activej.datastream.processor.StreamReducers.Reducer_Merge;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import io.activej.types.Types;
import org.junit.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.graph.StreamSchemas.simple;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.stream.DataflowTest.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertNotEquals;

public class ReducerDeadlockTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private ExecutorService executor;
	private ExecutorService sortingExecutor;

	@Before
	public void setUp() {
		executor = Executors.newSingleThreadExecutor();
		sortingExecutor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
		sortingExecutor.shutdownNow();
	}

	@Test
	public void test() throws IOException {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(List.of(new Partition(address1), new Partition(address2)))
				.bind(new Key<StreamCodec<Function<?, ?>>>() {}).toInstance(StreamCodecs.singleton(new TestKeyFunction()))
				.bind(new Key<StreamCodec<Comparator<?>>>() {}).toInstance(StreamCodecs.singleton(new TestComparator()))
				.bind(new Key<StreamCodec<Reducer<?, ?, ?, ?>>>() {}).to(Key.ofType(Types.parameterizedType(StreamCodec.class, Reducer_Merge.class)))
				.build();

		StreamConsumer_ToList<TestItem> result1 = StreamConsumer_ToList.create();
		StreamConsumer_ToList<TestItem> result2 = StreamConsumer_ToList.create();

		Module serverCommon = createCommonServer(common, executor, sortingExecutor);

		List<TestItem> list1 = new ArrayList<>(20000);
		for (int i = 0; i < 20000; i++) {
			list1.add(new TestItem(i * 2 + 2));
		}

		Module serverModule1 = ModuleBuilder.create()
				.install(serverCommon)
				.install(DatasetIdModule.create())
				.bind(Integer.class, "dataflowPort").toInstance(address1.getPort())
				.bind(datasetId("items")).toInstance(list1)
				.bind(datasetId("result")).toInstance(result1)
				.build();

		List<TestItem> list2 = new ArrayList<>(20000);
		for (int i = 0; i < 20000; i++) {
			list2.add(new TestItem(i * 2 + 1));
		}

		Module serverModule2 = ModuleBuilder.create()
				.install(serverCommon)
				.install(DatasetIdModule.create())
				.bind(Integer.class, "dataflowPort").toInstance(address2.getPort())
				.bind(datasetId("items")).toInstance(list2)
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class);

		server1.listen();
		server2.listen();

		Module clientCommon = createCommonClient(common);
		DataflowGraph graph = Injector.of(clientCommon).getInstance(DataflowGraph.class);

		SortedDataset<Long, TestItem> items = repartitionSort(sortedDatasetOfId("items",
				simple(TestItem.class), Long.class, new TestKeyFunction(), new TestComparator()));

		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(items, "result");

		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		// the sharder nonce is random, so with an *effectively zero* chance these assertions may fail
		assertNotEquals(result1.getList(), list1);
		assertNotEquals(result2.getList(), list2);
	}

	static InetSocketAddress getFreeListenAddress() throws UnknownHostException {
		return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), getFreePort());
	}
}
