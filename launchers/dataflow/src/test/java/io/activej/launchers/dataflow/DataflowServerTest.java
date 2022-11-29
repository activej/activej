package io.activej.launchers.dataflow;

import io.activej.config.Config;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.collector.ConcatCollector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.serializer.FunctionSerializer;
import io.activej.dataflow.proto.serializer.FunctionSubtypeSerializer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.graph.StreamSchemas.simple;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.serializer.ProtobufUtils.ofObject;
import static io.activej.launchers.dataflow.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class DataflowServerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private ExecutorService executor;
	private ExecutorService sortingExecutor;

	private int port1;
	private int port2;

	private TestServerLauncher serverLauncher1;
	private TestServerLauncher serverLauncher2;

	@Before
	public void setUp() {
		port1 = getFreePort();
		port2 = getFreePort();
		executor = Executors.newSingleThreadExecutor();
		sortingExecutor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() throws InterruptedException, ExecutionException {
		executor.shutdownNow();
		sortingExecutor.shutdownNow();

		serverLauncher1.shutdown();
		serverLauncher1.getCompleteFuture().toCompletableFuture().get();

		serverLauncher2.shutdown();
		serverLauncher2.getCompleteFuture().toCompletableFuture().get();
	}

	@Test
	public void testMapReduceSimple() throws Exception {
		launchServers(List.of("dog", "cat", "horse", "cat"), List.of("dog", "cat"), false);
		List<StringCount> result = new ArrayList<>();
		await(mapReduce(result));
		result.sort(StringCount.COMPARATOR);
		assertEquals(List.of(new StringCount("cat", 3), new StringCount("dog", 2), new StringCount("horse", 1)), result);
	}

	@Test
	public void testMapReduceBig() throws Exception {
		List<String> words1 = createStrings(100_000, 10_000);
		List<String> words2 = createStrings(100_000, 10_000);
		launchServers(words1, words2, false);
		List<StringCount> result = new ArrayList<>();

		await(mapReduce(result));

		result.sort(StringCount.COMPARATOR);

		List<StringCount> expected = Stream.concat(words1.stream(), words2.stream())
				.collect(groupingBy(Function.identity()))
				.entrySet()
				.stream()
				.map(e -> new StringCount(e.getKey(), e.getValue().size()))
				.sorted(StringCount.COMPARATOR)
				.collect(toList());

		assertEquals(expected, result);
	}

	@Test
	public void testMapReduceWithMalformedServer() throws Exception {
		launchServers(List.of("dog", "cat", "horse", "cat"), List.of("dog", "cat"), true);
		Exception exception = awaitException(mapReduce(new ArrayList<>()));

		assertThat(exception.getMessage(), containsString("Error on remote server"));
	}

	@Test
	public void testRepartitionAndSortSimple() throws Exception {
		List<String> result1 = new ArrayList<>();
		List<String> result2 = new ArrayList<>();
		launchServers(List.of("dog", "cat", "horse", "cat", "cow"), List.of("dog", "cat", "cow"), result1, result2, false);
		await(repartitionAndSort());

		List<String> result = new ArrayList<>();
		result.addAll(result1);
		result.addAll(result2);
		result.sort(Comparator.naturalOrder());

		assertEquals(List.of("cat", "cat", "cat", "cow", "cow", "dog", "dog", "horse"), result);
	}

	@Test
	public void testRepartitionAndSortBig() throws Exception {
		List<String> result1 = new ArrayList<>();
		List<String> result2 = new ArrayList<>();
		List<String> words1 = createStrings(100_000, 10_000);
		List<String> words2 = createStrings(100_000, 10_000);
		launchServers(words1, words2, result1, result2, false);
		await(repartitionAndSort());

		List<String> expected = new ArrayList<>(words1.size() + words2.size());
		expected.addAll(words1);
		expected.addAll(words2);
		expected.sort(Comparator.naturalOrder());


		List<String> result = new ArrayList<>(words1.size() + words2.size());
		result.addAll(result1);
		result.addAll(result2);
		result.sort(Comparator.naturalOrder());

		assertEquals(expected, result);
	}

	@Test
	public void testRepartitionAndSortWithMalformedServer() throws Exception {
		launchServers(List.of("dog", "cat", "horse", "cat", "cow"), List.of("dog", "cat", "cow"), true);
		Exception exception = awaitException(repartitionAndSort());

		assertThat(exception.getMessage(), containsString("Error on remote server"));
	}

	// region stubs & helpers
	private Promise<Void> mapReduce(List<StringCount> result) throws IOException {
		Partition partition1 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port1));
		Partition partition2 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port2));

		Injector injector = Injector.of(createModule(List.of(partition1, partition2)));

		DataflowClient client = injector.getInstance(DataflowClient.class);
		DataflowGraph graph = injector.getInstance(DataflowGraph.class);

		Dataset<String> items = datasetOfId("items", simple(String.class));
		Dataset<StringCount> mappedItems = map(items, new TestMapFunction(), simple(StringCount.class));
		Dataset<StringCount> reducedItems = splitSortReduceRepartitionReduce(mappedItems, new TestReducer(), new TestKeyFunction(), new TestComparator());
		Collector<StringCount> collector = ConcatCollector.create(reducedItems, client);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create(result);

		return graph.execute().both(resultSupplier.streamTo(resultConsumer))
				.whenException(resultConsumer::closeEx);
	}

	private Promise<Void> repartitionAndSort() throws IOException {
		Partition partition1 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port1));
		Partition partition2 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port2));

		Injector injector = Injector.of(createModule(List.of(partition1, partition2)));

		DataflowGraph graph = injector.getInstance(DataflowGraph.class);

		Dataset<String> items = datasetOfId("items", simple(String.class));
		Dataset<String> sorted = repartitionSort(localSort(items, String.class, new StringFunction(), new TestComparator()));
		DatasetConsumerOfId<String> consumerNode = consumerOfId(sorted, "result");
		consumerNode.channels(DataflowContext.of(graph));

		System.out.println(graph);
		return graph.execute();
	}

	@SuppressWarnings("SameParameterValue")
	private static List<String> createStrings(int howMany, int bound) {
		String[] strings = new String[howMany];
		Random random = new Random();
		for (int i = 0; i < howMany; i++) {
			strings[i] = Integer.toString(random.nextInt(bound));
		}
		return List.of(strings);
	}

	private void launchServers(List<String> server1Words, List<String> server2Words, boolean oneMalformed) {
		launchServers(server1Words, server2Words, new ArrayList<>(), new ArrayList<>(), oneMalformed);
	}

	private void launchServers(List<String> server1Words, List<String> server2Words, List<String> server1Result, List<String> server2Result, boolean oneMalformed) {
		serverLauncher1 = launchServer(port1, server1Words, server1Result, oneMalformed);
		serverLauncher2 = launchServer(port2, server2Words, server2Result, false);
	}

	private TestServerLauncher launchServer(int port, List<String> words, List<String> result, boolean malformed) {
		CountDownLatch latch = new CountDownLatch(1);
		TestServerLauncher launcher = new TestServerLauncher(port, words, result, malformed, latch);
		new Thread(() -> {
			try {
				launcher.launch(new String[0]);
			} catch (Exception e) {
				throw new AssertionError(e);
			}
		}).start();
		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
		return launcher;
	}

	public static class StringCount {
		static final Comparator<StringCount> COMPARATOR = Comparator.<StringCount, String>comparing(item -> item.s).thenComparingInt(item -> item.count);

		@Serialize
		public final String s;
		@Serialize
		public int count;

		public StringCount(@Deserialize("s") String s, @Deserialize("count") int count) {
			this.s = s;
			this.count = count;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof StringCount that)) return false;
			return s.equals(that.s) && count == that.count;
		}

		@Override
		public int hashCode() {
			return Objects.hash(s, count);
		}

		@Override
		public String toString() {
			return "StringCount{s='" + s + '\'' + ", count=" + count + '}';
		}
	}

	public static class TestReducer extends ReducerToAccumulator<String, StringCount, StringCount> {
		@Override
		public StringCount createAccumulator(String key) {
			return new StringCount(key, 0);
		}

		@Override
		public StringCount accumulate(StringCount accumulator, StringCount value) {
			accumulator.count += value.count;
			return accumulator;
		}

		@Override
		public StringCount combine(StringCount accumulator, StringCount anotherAccumulator) {
			accumulator.count += anotherAccumulator.count;
			return accumulator;
		}
	}

	public static class TestMapFunction implements Function<String, StringCount> {
		@Override
		public StringCount apply(String s) {
			return new StringCount(s, 1);
		}
	}

	public static class TestKeyFunction implements Function<StringCount, String> {
		@Override
		public String apply(StringCount stringCount) {
			return stringCount.s;
		}
	}

	public static class TestComparator implements Comparator<String> {
		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
	}

	public static class StringFunction implements Function<String, String> {
		@Override
		public String apply(String string) {
			return string;
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static Module createModule(List<Partition> partitions) {
		return ModuleBuilder.create()
				.install(DataflowModule.create())
				.install(DatasetIdModule.create())
				.bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to(() -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();

					serializer.setSubtypeCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new));
					serializer.setSubtypeCodec(TestMapFunction.class, ofObject(TestMapFunction::new));
					serializer.setSubtypeCodec(StringFunction.class, ofObject(StringFunction::new));

					return serializer;
				})
				.bind(new Key<BinarySerializer<Comparator<?>>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<BinarySerializer<ReducerToResult>>() {}).toInstance(ofObject(TestReducer::new))
				.bind(new Key<BinarySerializer<Reducer<?, ?, ?, ?>>>() {}).to((inputToAccumulatorSerializer, accumulatorToOutputSerializer, mergeReducerSerializer) -> {
							FunctionSubtypeSerializer<Reducer> serializer = FunctionSubtypeSerializer.create();

							serializer.setSubtypeCodec(InputToAccumulator.class, inputToAccumulatorSerializer);
							serializer.setSubtypeCodec(AccumulatorToOutput.class, accumulatorToOutputSerializer);
							serializer.setSubtypeCodec(MergeReducer.class, mergeReducerSerializer);

							return ((BinarySerializer) serializer);
						},
						new Key<BinarySerializer<InputToAccumulator>>() {},
						new Key<BinarySerializer<AccumulatorToOutput>>() {},
						new Key<BinarySerializer<MergeReducer>>() {})
				.scan(new Object() {
					@Provides
					DataflowClient client(ByteBufsCodec<DataflowResponse, DataflowRequest> codec, BinarySerializerLocator serializers, FunctionSerializer functionSerializer) {
						return DataflowClient.create(codec, serializers, functionSerializer);
					}

					@Provides
					DataflowGraph graph(DataflowClient client) {
						return new DataflowGraph(client, partitions);
					}
				})
				.build();
	}

	private static final class TestServerLauncher extends DataflowServerLauncher {
		private final int port;
		private final List<String> words;
		private final List<String> result;
		private final boolean malformed;
		private final CountDownLatch latch;

		private TestServerLauncher(int port, List<String> words, List<String> result, boolean malformed, CountDownLatch latch) {
			this.port = port;
			this.words = words;
			this.result = result;
			this.malformed = malformed;
			this.latch = latch;
		}

		@Override
		protected Module getOverrideModule() {
			return createModule(List.of())
					.overrideWith(ModuleBuilder.create()
							.bind(datasetId(malformed ? "" : "items")).toInstance(words)
							.bind(Config.class).toInstance(Config.create().with("dataflow.server.listenAddresses", String.valueOf(port)))
							.bind(Eventloop.class).toInstance(Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrow()))
							.bind(Executor.class).toInstance(Executors.newSingleThreadExecutor())
							.bind(datasetId("result")).toInstance(StreamConsumerToList.create(result))
							.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
							.build());
		}

		@Override
		protected void run() throws Exception {
			latch.countDown();
			awaitShutdown();
		}
	}
	// endregion
}
