package io.activej.dataflow.stream;

import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.collector.Collector;
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
import io.activej.dataflow.inject.SortingExecutor;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.FunctionSerializer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.AsyncHttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import io.activej.types.Types;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.common.Utils.concat;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.ProtobufUtils.ofObject;
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

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

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

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(3),
						new TestItem(5)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
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

		Dataset<TestItem> items = datasetOfId("items", TestItem.class);
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

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(new Key<BinarySerializer<StreamReducers.Reducer<?, ?, ?, ?>>>() {}).to(Key.ofType(Types.parameterizedType(BinarySerializer.class, MergeReducer.class)))
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
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
				TestItem.class, Long.class, new TestKeyFunction(), new TestComparator()));
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

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(partition1, partition2, partition3))
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
				.bind(datasetId("items")).toInstance(list1)
				.bind(datasetId("result")).toInstance(result1)
				.build();

		List<TestItem> list2 = List.of(
				new TestItem(21),
				new TestItem(26));
		Module serverModule2 = ModuleBuilder.create()
				.install(common)
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
						datasetOfId("items", TestItem.class),
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

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
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

		Dataset<TestItem> filterDataset = filter(datasetOfId("items", TestItem.class), new TestPredicate());
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

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(List.of(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
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

		Dataset<TestItem> filterDataset = filter(datasetOfId("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());

		Collector<TestItem> collector = new Collector<>(sortedDataset, client);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)), resultConsumer.getList());
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

	static ModuleBuilder createCommon(Executor executor, Executor sortingExecutor, Path secondaryPath, List<Partition> graphPartitions) {
		return ModuleBuilder.create()
				.install(DataflowModule.create())
				.bind(Executor.class, SortingExecutor.class).toInstance(sortingExecutor)
				.bind(Executor.class).toInstance(executor)
				.bind(Eventloop.class).toInstance(Eventloop.getCurrentEventloop())
				.scan(new Object() {
					@Provides
					DataflowServer server(Eventloop eventloop, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerModule.BinarySerializerLocator serializers, FunctionSerializer functionSerializer, Injector environment) {
						return DataflowServer.create(eventloop, codec, serializers, environment, functionSerializer);
					}

					@Provides
					DataflowClient client(Executor executor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, BinarySerializerModule.BinarySerializerLocator serializers, FunctionSerializer functionSerializer) {
						return new DataflowClient(executor, secondaryPath, codec, serializers, functionSerializer);
					}

					@Provides
					@Transient
					DataflowGraph graph(DataflowClient client) {
						return new DataflowGraph(client, graphPartitions);
					}

					@Provides
					AsyncHttpClient httpClient(Eventloop eventloop) {
						return AsyncHttpClient.create(eventloop);
					}

					@Provides
					AsyncHttpServer debugServer(Eventloop eventloop, Executor executor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, Injector env) {
						return AsyncHttpServer.create(eventloop, new DataflowDebugServlet(graphPartitions, executor, codec, env));
					}
				});
	}

	static InetSocketAddress getFreeListenAddress() {
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
				.bind(new Key<BinarySerializer<Comparator<?>>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<BinarySerializer<Function<?, ?>>>() {}).toInstance(ofObject(TestKeyFunction::new))
				.bind(new Key<BinarySerializer<Predicate<?>>>() {}).toInstance(ofObject(TestPredicate::new))
				.build();
	}
}
