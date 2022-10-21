package io.activej.dataflow.stream;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.collector.MergeCollector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.proto.serializer.FunctionSubtypeSerializer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.serializer.ProtobufUtils.ofObject;
import static io.activej.dataflow.stream.DataflowTest.createCommon;
import static io.activej.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static java.util.Comparator.naturalOrder;
import static org.junit.Assert.assertEquals;

public class MapReduceTest {

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

	public static class StringCount {
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

	@Test
	public void test() throws Exception {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						"dog",
						"cat",
						"horse",
						"cat"))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(List.of(
						"dog",
						"cat"))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<String> items = datasetOfId("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new StringMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sortReduceRepartitionReduce(mappedItems,
				new StringReducer(), String.class, new StringKeyFunction(), Comparator.naturalOrder());
		Collector<StringCount> collector = MergeCollector.create(reducedItems, client, new StringKeyFunction(), naturalOrder(), false);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		System.out.println(resultConsumer.getList());

		assertEquals(Set.of(
				new StringCount("cat", 3),
				new StringCount("dog", 2),
				new StringCount("horse", 1)), new HashSet<>(resultConsumer.getList()));
	}

	public static class StringReducer extends ReducerToAccumulator<String, StringCount, StringCount> {
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

	public static class StringMapFunction implements Function<String, StringCount> {
		@Override
		public StringCount apply(String s) {
			return new StringCount(s, 1);
		}
	}

	public static class StringKeyFunction implements Function<StringCount, String> {
		@Override
		public String apply(StringCount stringCount) {
			return stringCount.s;
		}
	}

	@SuppressWarnings({"rawtypes", "NullableProblems", "unchecked"})
	private static Module createSerializersModule() {
		return ModuleBuilder.create()
				.bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to(() -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec(StringKeyFunction.class, ofObject(StringKeyFunction::new));
					serializer.setSubtypeCodec(StringMapFunction.class, ofObject(StringMapFunction::new));
					return serializer;
				})
				.bind(new Key<BinarySerializer<Comparator<?>>>() {}).toInstance(ofObject(Comparator::naturalOrder))
				.bind(new Key<BinarySerializer<Reducer<?, ?, ?, ?>>>() {}).to((inputToAccumulator, accumulatorToOutput) -> {
							FunctionSubtypeSerializer<Reducer> serializer = FunctionSubtypeSerializer.create();
							serializer.setSubtypeCodec(InputToAccumulator.class, inputToAccumulator);
							serializer.setSubtypeCodec(AccumulatorToOutput.class, accumulatorToOutput);
							return ((BinarySerializer) serializer);
						},
						new Key<BinarySerializer<InputToAccumulator>>() {},
						new Key<BinarySerializer<AccumulatorToOutput>>() {})
				.bind(new Key<BinarySerializer<ReducerToResult>>() {}).toInstance(ofObject(StringReducer::new))
				.build();
	}
}
