package io.activej.dataflow.stream;

import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.collector.AsyncCollector;
import io.activej.dataflow.collector.ConcatCollector;
import io.activej.dataflow.collector.MergeCollector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.http.DataflowDebugServlet;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.inject.SortingExecutor;
import io.activej.dataflow.messaging.DataflowRequest;
import io.activej.dataflow.messaging.DataflowResponse;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpClient;
import io.activej.http.HttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import io.activej.types.Types;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.common.Utils.concat;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.graph.StreamSchemas.simple;
import static io.activej.dataflow.helper.MeergeStubStreamSorterStorage.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Comparator.comparing;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class DataflowTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

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
	public void testForward() throws Exception {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(3),
						new TestItem(5)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(2),
						new TestItem(4),
						new TestItem(6)
				))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> items = datasetOfId("items", simple(TestItem.class));
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(items, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(new TestItem(1), new TestItem(3), new TestItem(5)), result1.getList());
		assertEquals(List.of(new TestItem(2), new TestItem(4), new TestItem(6)), result2.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(new Key<StreamCodec<Reducer<?, ?, ?, ?>>>() {}).to(Key.ofType(Types.parameterizedType(StreamCodec.class, MergeReducer.class)))
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5),
						new TestItem(6)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(6)))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, TestItem> items = repartitionSort(sortedDatasetOfId("items",
				simple(TestItem.class), Long.class, new TestKeyFunction(), new TestComparator()));
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(items, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		List<TestItem> results = new ArrayList<>();
		results.addAll(result1.getList());
		results.addAll(result2.getList());
		results.sort(Comparator.comparingLong(item -> item.value));

		assertEquals(List.of(
				new TestItem(1),
				new TestItem(1),
				new TestItem(2),
				new TestItem(3),
				new TestItem(4),
				new TestItem(5),
				new TestItem(6),
				new TestItem(6)), results);
	}

	@Test
	public void testRepartitionWithFurtherSort() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		InetSocketAddress address3 = getFreeListenAddress();

		Partition partition1 = new Partition(address1);
		Partition partition2 = new Partition(address2);
		Partition partition3 = new Partition(address3);

		Module common = createCommon(executor, sortingExecutor, List.of(partition1, partition2, partition3))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result3 = StreamConsumerToList.create();

		List<TestItem> list1 = List.of(
				new TestItem(15),
				new TestItem(12),
				new TestItem(13),
				new TestItem(17),
				new TestItem(11),
				new TestItem(13));
		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(list1)
				.bind(datasetId("result")).toInstance(result1)
				.build();

		List<TestItem> list2 = List.of(
				new TestItem(21),
				new TestItem(26));
		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(list2)
				.bind(datasetId("result")).toInstance(result2)
				.build();

		List<TestItem> list3 = List.of(
				new TestItem(33),
				new TestItem(35),
				new TestItem(31),
				new TestItem(38),
				new TestItem(36));
		Module serverModule3 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(list3)
				.bind(datasetId("result")).toInstance(result3)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);
		DataflowServer server3 = Injector.of(serverModule3).getInstance(DataflowServer.class).withListenAddress(address3);

		server1.listen();
		server2.listen();
		server3.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> items = localSort(
				repartition(
						datasetOfId("items", simple(TestItem.class)),
						new TestKeyFunction(),
						List.of(partition2, partition3)
				),
				Long.class,
				new TestKeyFunction(),
				new TestComparator()
		);
		Dataset<TestItem> consumerNode = consumerOfId(items, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
					server3.close();
				})));

		Set<TestItem> expectedOnServers2And3 = new HashSet<>();
		expectedOnServers2And3.addAll(list1);
		expectedOnServers2And3.addAll(list2);
		expectedOnServers2And3.addAll(list3);

		assertTrue(isSorted(result2.getList(), comparing(testItem -> testItem.value)));
		assertTrue(isSorted(result3.getList(), comparing(testItem -> testItem.value)));

		Set<TestItem> actualOnServers2And3 = new HashSet<>(concat(result2.getList(), result3.getList()));
		assertEquals(expectedOnServers2And3, actualOnServers2And3);
		assertTrue(result1.getList().isEmpty());
	}

	@Test
	public void testFilter() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(6),
						new TestItem(4),
						new TestItem(2),
						new TestItem(3),
						new TestItem(1)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(7),
						new TestItem(7),
						new TestItem(8),
						new TestItem(2),
						new TestItem(5)))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> filterDataset = filter(datasetOfId("items", simple(TestItem.class)), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(sortedDataset, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(new TestItem(2), new TestItem(4), new TestItem(6)), result1.getList());
		assertEquals(List.of(new TestItem(2), new TestItem(8)), result2.getList());
	}

	@Test
	public void testCollector() throws Exception {
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(6),
						new TestItem(7),
						new TestItem(8),
						new TestItem(9),
						new TestItem(10)))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<TestItem> filterDataset = filter(datasetOfId("items", simple(TestItem.class)), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());

		AsyncCollector<TestItem> collector = ConcatCollector.create(sortedDataset, client);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)), resultConsumer.getList());
	}

	@Test
	public void testOffsetLimit() throws Exception {
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(new Key<StreamCodec<Reducer<?, ?, ?, ?>>>() {}).to(Key.ofType(Types.parameterizedType(StreamCodec.class, MergeReducer.class)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(6),
						new TestItem(7),
						new TestItem(8),
						new TestItem(9),
						new TestItem(10)))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<TestItem> dataset = datasetOfId("items", simple(TestItem.class));
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(dataset, long.class, new TestKeyFunction(), new TestComparator());
		SortedDataset<Long, TestItem> afterOffsetAndLimitApplied = offsetLimit(sortedDataset, 3, 4);

		AsyncCollector<TestItem> collector = MergeCollector.create(afterOffsetAndLimitApplied, client, false);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(new TestItem(4), new TestItem(5), new TestItem(6), new TestItem(7)), resultConsumer.getList());
	}

	@Test
	public void testEmpty() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		Dataset<TestItem> emptyDataset = empty(simple(TestItem.class));
		DatasetConsumerOfId<TestItem> consumerNode = consumerOfId(emptyDataset, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertTrue(result1.getList().isEmpty());
		assertTrue(result2.getList().isEmpty());
	}

	@Test
	public void testUnion() throws Exception {
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(new Key<StreamCodec<Reducer<?, ?, ?, ?>>>() {}).to(Key.ofType(Types.parameterizedType(StreamCodec.class, MergeReducer.class)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items1")).toInstance(List.of(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3)))
				.bind(datasetId("items2")).toInstance(List.of(
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items1")).toInstance(List.of(
						new TestItem(1),
						new TestItem(6),
						new TestItem(7)))
				.bind(datasetId("items2")).toInstance(List.of(
						new TestItem(1),
						new TestItem(5),
						new TestItem(8)))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<TestItem> dataset1 = datasetOfId("items1", simple(TestItem.class));
		SortedDataset<Long, TestItem> sorted1 = repartitionSort(localSort(dataset1, Long.class, new TestKeyFunction(), Comparator.naturalOrder()));
		Dataset<TestItem> dataset2 = datasetOfId("items2", simple(TestItem.class));
		SortedDataset<Long, TestItem> sorted2 = repartitionSort(localSort(dataset2, Long.class, new TestKeyFunction(), Comparator.naturalOrder()));

		SortedDataset<Long, TestItem> union = union(sorted1, sorted2);

		MergeCollector<Long, TestItem> collector = MergeCollector.create(union, client, false);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(
				new TestItem(1),
				new TestItem(2),
				new TestItem(3),
				new TestItem(4),
				new TestItem(5),
				new TestItem(6),
				new TestItem(7),
				new TestItem(8)
		), resultConsumer.getList());
	}

	@Test
	public void testUnionAll() throws Exception {
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(new Key<StreamCodec<Reducer<?, ?, ?, ?>>>() {}).to(Key.ofType(Types.parameterizedType(StreamCodec.class, MergeReducer.class)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items1")).toInstance(List.of(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3)))
				.bind(datasetId("items2")).toInstance(List.of(
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items1")).toInstance(List.of(
						new TestItem(1),
						new TestItem(6),
						new TestItem(7)))
				.bind(datasetId("items2")).toInstance(List.of(
						new TestItem(1),
						new TestItem(5),
						new TestItem(8)))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<TestItem> dataset1 = datasetOfId("items1", simple(TestItem.class));
		Dataset<TestItem> dataset2 = datasetOfId("items2", simple(TestItem.class));

		Dataset<TestItem> union = unionAll(dataset1, dataset2);

		SortedDataset<Long, TestItem> sortedUnion = repartitionSort(localSort(union, Long.class, new TestKeyFunction(), Comparator.naturalOrder()));

		MergeCollector<Long, TestItem> collector = MergeCollector.create(sortedUnion, client, false);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(
				new TestItem(1),
				new TestItem(1),
				new TestItem(1),
				new TestItem(2),
				new TestItem(3),
				new TestItem(3),
				new TestItem(4),
				new TestItem(5),
				new TestItem(5),
				new TestItem(6),
				new TestItem(7),
				new TestItem(8)
		), resultConsumer.getList());
	}

	@SerializeRecord
	public record TestItem(long value) {}

	public static class TestComparator implements Comparator<Long> {
		@Override
		public int compare(Long o1, Long o2) {
			return o1.compareTo(o2);
		}
	}

	public static class TestKeyFunction implements Function<TestItem, Long> {
		@Override
		public Long apply(TestItem item) {
			return item.value;
		}
	}

	private static class TestPredicate implements Predicate<TestItem> {
		@Override
		public boolean test(TestItem input) {
			return input.value % 2 == 0;
		}
	}

	public static ModuleBuilder createCommon(Executor executor, Executor sortingExecutor, List<Partition> graphPartitions) {
		return ModuleBuilder.create()
				.install(DataflowModule.create())
				.bind(Executor.class, SortingExecutor.class).toInstance(sortingExecutor)
				.bind(Executor.class).toInstance(executor)
				.bind(NioReactor.class).toInstance(Reactor.getCurrentReactor())
				.scan(new Object() {
					@Provides
					DataflowServer server(NioReactor reactor, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerModule.BinarySerializerLocator serializers, Injector environment) {
						return DataflowServer.create(reactor, codec, serializers, environment);
					}

					@Provides
					DataflowClient client(NioReactor reactor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec,
							BinarySerializerModule.BinarySerializerLocator serializers) {
						return DataflowClient.create(reactor, codec, serializers);
					}

					@Provides
					@Transient
					DataflowGraph graph(NioReactor reactor, DataflowClient client) {
						return new DataflowGraph(reactor, client, graphPartitions);
					}

					@Provides
					AsyncHttpClient httpClient(NioReactor reactor) {
						return HttpClient.create(reactor);
					}

					@Provides
					HttpServer debugServer(NioReactor reactor, Executor executor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, Injector env) {
						return HttpServer.create(reactor, new DataflowDebugServlet(graphPartitions, executor, codec, env));
					}
				});
	}

	public static InetSocketAddress getFreeListenAddress() {
		try {
			return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), getFreePort());
		} catch (UnknownHostException ignored) {
			throw new AssertionError();
		}
	}

	private static <T> boolean isSorted(Collection<T> collection, Comparator<T> comparator) {
		if (collection.size() < 2) return true;
		Iterator<T> iterator = collection.iterator();
		T current = iterator.next();
		while (iterator.hasNext()) {
			T next = iterator.next();
			if (comparator.compare(current, next) > 0) {
				return false;
			}
			current = next;
		}
		return true;
	}

	private static Module createSerializersModule() {
		return ModuleBuilder.create()
				.bind(new Key<StreamCodec<Comparator<?>>>() {}).toInstance(StreamCodecs.singleton(new TestComparator()))
				.bind(new Key<StreamCodec<Function<?, ?>>>() {}).toInstance(StreamCodecs.singleton(new TestKeyFunction()))
				.bind(new Key<StreamCodec<Predicate<?>>>() {}).toInstance(StreamCodecs.singleton(new TestPredicate()))
				.build();
	}
}
