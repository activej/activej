package io.activej.dataflow.stream;

import io.activej.common.Utils;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.collector.MergeCollector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.DatasetId;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.proto.FunctionSubtypeSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamFilter;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.activej.common.Utils.intersection;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.ProtobufUtils.ofObject;
import static io.activej.dataflow.stream.DataflowTest.createCommon;
import static io.activej.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.*;
import static io.activej.test.TestUtils.assertCompleteFn;
import static java.util.Comparator.naturalOrder;
import static org.junit.Assert.*;

public class CrdtMapReduceTest {

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
	public void testCrdtSource() throws Exception {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.install(new AbstractModule() {

					@Provides
					CrdtStorage<Integer, Set<String>> sourceStorage(Eventloop eventloop) {
						return CrdtStorageMap.create(eventloop, CrdtFunction.ignoringTimestamp(Utils::union));
					}

					@Provides
					@DatasetId("items")
					@Transient
					StreamSupplier<String> source(CrdtStorage<Integer, Set<String>> storage) {
						return StreamSupplier.ofPromise(storage.download()
								.map(supplier -> supplier.transformWith(new StreamFilter<>() {
									@Override
									protected @NotNull StreamDataAcceptor<CrdtData<Integer, Set<String>>> onResumed(@NotNull StreamDataAcceptor<String> output) {
										return crdtData -> {
											for (String string : crdtData.getState()) {
												output.accept(string);
											}
										};
									}
								})));
					}

				})
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.transform(new KeyPattern<CrdtStorage<Integer, Set<String>>>() {}, (bindings, scope, key, binding) -> binding
						.onInstance(storage -> {
							if (!(storage instanceof CrdtStorageMap<Integer, Set<String>> map)) return;

							map.put(1, Set.of("dog"));
							map.put(2, Set.of("dog", "cat"));
							map.put(3, Set.of("cat", "horse"));
							map.put(4, Set.of("cat", "dog", "rabbit"));
						}))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.transform(new KeyPattern<CrdtStorage<Integer, Set<String>>>() {}, (bindings, scope, key, binding) -> binding
						.onInstance(storage -> {
							if (!(storage instanceof CrdtStorageMap<Integer, Set<String>> map)) return;

							map.put(1, Set.of("fish"));
							map.put(2, Set.of("cat", "dog"));
							map.put(8, Set.of("cat", "horse"));
						}))
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
		MergeCollector<String, StringCount> collector = new MergeCollector<>(reducedItems, client, new StringKeyFunction(), naturalOrder(), false);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		assertEquals(List.of(
				new StringCount("cat", 5),
				new StringCount("dog", 4),
				new StringCount("fish", 1),
				new StringCount("horse", 2),
				new StringCount("rabbit", 1)), resultConsumer.getList());
	}

	@Test
	public void testCrdtSink() throws Exception {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.install(new AbstractModule() {

					@Provides
					CrdtStorage<String, Integer> sourceStorage(Eventloop eventloop) {
						return CrdtStorageMap.create(eventloop, CrdtFunction.ignoringTimestamp(Integer::max));
					}

					@Provides
					@DatasetId("result")
					@Transient
					StreamConsumer<StringCount> sink(CrdtStorage<String, Integer> storage) {
						return StreamConsumer.ofPromise(storage.upload()
								.map(consumer -> {
									long timestamp = Eventloop.getCurrentEventloop().currentTimeMillis();
									return consumer.transformWith(StreamFilter.mapper(stringCount -> new CrdtData<>(stringCount.s, timestamp, stringCount.count)));
								}));
					}
				})
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(List.of(
						"dog",
						"cat",
						"horse",
						"cat"))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(List.of(
						"dog",
						"cat"))
				.build();

		Injector serverInjector1 = Injector.of(serverModule1);
		DataflowServer server1 = serverInjector1.getInstance(DataflowServer.class).withListenAddress(address1);

		Injector serverInjector2 = Injector.of(serverModule2);
		DataflowServer server2 = serverInjector2.getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<String> items = datasetOfId("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new StringMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sortReduceRepartitionReduce(mappedItems,
				new StringReducer(), String.class, new StringKeyFunction(), Comparator.naturalOrder());
		DatasetConsumerOfId<StringCount> consumerNode = consumerOfId(reducedItems, "result");
		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		Key<CrdtStorage<String, Integer>> storageKey = new Key<>() {};

		Map<String, Integer> actual1 = toMap(serverInjector1.getInstance(storageKey));
		Map<String, Integer> actual2 = toMap(serverInjector2.getInstance(storageKey));

		assertFalse(actual1.isEmpty());
		assertFalse(actual2.isEmpty());

		assertTrue(intersection(actual1.keySet(), actual2.keySet()).isEmpty());

		assertEquals(1, getFromAny(actual1, actual2, "horse"));
		assertEquals(2, getFromAny(actual1, actual2, "dog"));
		assertEquals(3, getFromAny(actual1, actual2, "cat"));
	}

	@Test
	public void testCrdtSingleSourceStorage() throws Exception {
		InetSocketAddress crdtAddress = getFreeListenAddress();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(new Partition(address1), new Partition(address2)))
				.install(createSerializersModule())
				.install(new AbstractModule() {

					@Provides
					CrdtStorage<Integer, Set<String>> sourceStorage(Eventloop eventloop, CrdtDataSerializer<Integer, Set<String>> serializer) {
						return CrdtStorageClient.create(eventloop, crdtAddress, serializer);
					}

					@Provides
					@DatasetId("items")
					@Transient
					StreamSupplier<String> source(CrdtStorage<Integer, Set<String>> storage) {
						return StreamSupplier.ofPromise(storage.download()
								.map(supplier -> supplier.transformWith(new StreamFilter<>() {
									@Override
									protected @NotNull StreamDataAcceptor<CrdtData<Integer, Set<String>>> onResumed(@NotNull StreamDataAcceptor<String> output) {
										return crdtData -> {
											for (String string : crdtData.getState()) {
												output.accept(string);
											}
										};
									}
								})));
					}

				})
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module crdtServerModule = ModuleBuilder.create()
				.bind(new Key<CrdtDataSerializer<Integer, Set<String>>>() {}).toInstance(new CrdtDataSerializer<>(INT_SERIALIZER, ofSet(UTF8_SERIALIZER)))
				.install(new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.getCurrentEventloop();
					}

					@Provides
					CrdtStorage<Integer, Set<String>> storage(Eventloop eventloop) {
						return CrdtStorageMap.create(eventloop, CrdtFunction.ignoringTimestamp(Utils::union));
					}

					@Provides
					CrdtServer<Integer, Set<String>> server(Eventloop eventloop, CrdtStorage<Integer, Set<String>> storage, CrdtDataSerializer<Integer, Set<String>> serializer) {
						return CrdtServer.create(eventloop, storage, serializer);
					}
				})
				.transform(new KeyPattern<CrdtStorage<Integer, Set<String>>>() {}, (bindings, scope, key, binding) -> binding
						.onInstance(storage -> {
							if (!(storage instanceof CrdtStorageMap<Integer, Set<String>> map)) return;

							map.put(1, Set.of("dog"));
							map.put(2, Set.of("dog", "cat"));
							map.put(3, Set.of("cat", "horse"));
							map.put(4, Set.of("cat", "dog", "rabbit"));
							map.put(5, Set.of("fish"));
							map.put(6, Set.of("cat", "dog"));
							map.put(7, Set.of("cat", "horse"));
						}))
				.build();

		CrdtServer<Integer, Set<String>> crdtServer = Injector.of(crdtServerModule).getInstance(new Key<CrdtServer<Integer, Set<String>>>() {}).withListenAddress(crdtAddress);
		DataflowServer server1 = Injector.of(common).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(common).getInstance(DataflowServer.class).withListenAddress(address2);

		crdtServer.listen();
		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		List<Partition> partitions = graph.getAvailablePartitions();
		assertTrue(partitions.size() > 1);
		// a single random partition to download data
		partitions = List.of(partitions.get(ThreadLocalRandom.current().nextInt(partitions.size())));

		Dataset<String> items = datasetOfId("items", String.class, partitions);
		Dataset<String> repartitioned = repartition(items, new StringIdentityFunction());
		Dataset<StringCount> mappedItems = map(repartitioned, new StringMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sortReduceRepartitionReduce(mappedItems,
				new StringReducer(), String.class, new StringKeyFunction(), Comparator.naturalOrder());
		MergeCollector<String, StringCount> collector = new MergeCollector<>(reducedItems, client, new StringKeyFunction(), naturalOrder(), false);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();

		resultSupplier.streamTo(resultConsumer).whenComplete(assertCompleteFn());

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					crdtServer.close();
					server1.close();
					server2.close();
				})));

		System.out.println(graph.toGraphViz());

		assertEquals(List.of(
				new StringCount("cat", 5),
				new StringCount("dog", 4),
				new StringCount("fish", 1),
				new StringCount("horse", 2),
				new StringCount("rabbit", 1)), resultConsumer.getList());
	}

	private static Map<String, Integer> toMap(CrdtStorage<String, Integer> storage) {
		assert storage instanceof CrdtStorageMap<String, Integer>;

		Map<String, Integer> result = new HashMap<>();
		((CrdtStorageMap<String, Integer>) storage).iterator()
				.forEachRemaining(crdtData -> result.put(crdtData.getKey(), crdtData.getState()));
		return result;
	}

	private static int getFromAny(Map<String, Integer> map1, Map<String, Integer> map2, String key) {
		Integer result1 = map1.get(key);
		if (result1 != null) return result1;

		return map2.get(key);
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

	public static class StringIdentityFunction implements Function<String, String> {
		@Override
		public String apply(String s) {
			return s;
		}
	}

	@SuppressWarnings({"rawtypes", "NullableProblems", "unchecked"})
	private static Module createSerializersModule() {
		return ModuleBuilder.create()
				.bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to(() -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec(StringKeyFunction.class, ofObject(StringKeyFunction::new));
					serializer.setSubtypeCodec(StringMapFunction.class, ofObject(StringMapFunction::new));
					serializer.setSubtypeCodec(StringIdentityFunction.class, ofObject(StringIdentityFunction::new));
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
				.bind(new Key<CrdtDataSerializer<Integer, Set<String>>>() {}).toInstance(new CrdtDataSerializer<>(INT_SERIALIZER, ofSet(UTF8_SERIALIZER)))
				.build();
	}
}
