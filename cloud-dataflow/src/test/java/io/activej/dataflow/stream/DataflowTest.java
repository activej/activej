package io.activej.dataflow.stream;

import io.activej.codec.StructuredCodec;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetListConsumer;
import io.activej.dataflow.di.BinarySerializerModule;
import io.activej.dataflow.di.CodecsModule.Subtypes;
import io.activej.dataflow.di.DataflowModule;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.server.Collector;
import io.activej.dataflow.server.DataflowClient;
import io.activej.dataflow.server.DataflowServer;
import io.activej.dataflow.server.command.DatagraphCommand;
import io.activej.dataflow.server.command.DatagraphResponse;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.di.Injector;
import io.activej.di.Key;
import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.di.module.ModuleBuilder;
import io.activej.eventloop.Eventloop;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.codec.StructuredCodec.ofObject;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.di.DatasetIdImpl.datasetId;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class DataflowTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor;

	@Before
	public void setUp() {
		executor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void testForward() throws Exception {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(3),
						new TestItem(5)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
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

		Dataset<TestItem> items = datasetOfList("items", TestItem.class);
		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(asList(new TestItem(1), new TestItem(3), new TestItem(5)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result2.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2))).build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
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
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(6)))
				.bind(datasetId("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, TestItem> items = repartition_Sort(sortedDatasetOfList("items",
				TestItem.class, Long.class, new TestKeyFunction(), new TestComparator()));
		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		List<TestItem> results = new ArrayList<>();
		results.addAll(result1.getList());
		results.addAll(result2.getList());
		results.sort(Comparator.comparingLong(item -> item.value));

		assertEquals(asList(
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
	public void testFilter() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(6),
						new TestItem(4),
						new TestItem(2),
						new TestItem(3),
						new TestItem(1)))
				.bind(datasetId("result")).toInstance(result1)
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
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

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());
		DatasetListConsumer<?> consumerNode = listConsumer(sortedDataset, "result");
		consumerNode.compileInto(graph);

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(8)), result2.getList());
	}

	@Test
	public void testCollector() throws Exception {
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
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

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());

		Collector<TestItem> collector = new Collector<>(sortedDataset, client);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		resultSupplier.streamTo(resultConsumer).whenComplete(assertComplete());

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)), resultConsumer.getList());
	}

	public static final class TestItem {
		@Serialize(order = 0)
		public final long value;

		public TestItem(@Deserialize("value") long value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "TestItem{value=" + value + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestItem other = (TestItem) o;
			return value == other.value;
		}

		@Override
		public int hashCode() {
			return (int) (value ^ (value >>> 32));
		}
	}

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

	static ModuleBuilder createCommon(Executor executor, Path secondaryPath, List<Partition> graphPartitions) {
		return ModuleBuilder.create()
				.install(DataflowModule.create())
				.bind(Executor.class).toInstance(executor)
				.bind(Eventloop.class).toInstance(Eventloop.getCurrentEventloop())
				.scan(new Object() {

					@Provides
					DataflowServer server(Eventloop eventloop, ByteBufsCodec<DatagraphCommand, DatagraphResponse> codec, BinarySerializerModule.BinarySerializerLocator serializers, Injector environment) {
						return new DataflowServer(eventloop, codec, serializers, environment);
					}

					@Provides
					DataflowClient client(Executor executor, ByteBufsCodec<DatagraphResponse, DatagraphCommand> codec, BinarySerializerModule.BinarySerializerLocator serializers) {
						return new DataflowClient(executor, secondaryPath, codec, serializers);
					}

					@Provides
					DataflowGraph graph(DataflowClient client, @Subtypes StructuredCodec<Node> nodeCodec) {
						return new DataflowGraph(client, graphPartitions, nodeCodec);
					}
				})
				.bind(new Key<StructuredCodec<TestComparator>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<StructuredCodec<TestKeyFunction>>() {}).toInstance(ofObject(TestKeyFunction::new))
				.bind(new Key<StructuredCodec<TestPredicate>>() {}).toInstance(ofObject(TestPredicate::new));
	}

	static InetSocketAddress getFreeListenAddress() throws UnknownHostException {
		return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), getFreePort());
	}
}
