package io.activej.dataflow.stream;

import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.dataflow.proto.FunctionSubtypeSerializer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamJoin.InnerJoiner;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.http.AsyncHttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.ProtobufUtils.ofObject;
import static io.activej.dataflow.stream.DataflowTest.createCommon;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

public class PageRankTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private InetSocketAddress address1;
	private InetSocketAddress address2;

	private ExecutorService executor;
	private ExecutorService sortingExecutor;

	@Before
	public void setUp() {
		address1 = new InetSocketAddress(getFreePort());
		address2 = new InetSocketAddress(getFreePort());

		executor = Executors.newCachedThreadPool();
		sortingExecutor = Executors.newCachedThreadPool();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
		sortingExecutor.shutdownNow();
	}

	@SerializeRecord
	public record Page(long pageId, long[] links) {

		public void disperse(Rank rank, StreamDataAcceptor<Rank> cb) {
			for (long link : links) {
				Rank newRank = new Rank(link, rank.value / links.length);
				cb.accept(newRank);
			}
		}
	}

	public static class PageKeyFunction implements Function<Page, Long> {
		@Override
		public Long apply(Page page) {
			return page.pageId;
		}
	}

	@SerializeRecord
	public record Rank(long pageId, double value) {
		@SuppressWarnings({"SimplifiableIfStatement", "EqualsWhichDoesntCheckParameterClass"})
		@Override
		public boolean equals(Object o) {
			Rank rank = (Rank) o;
			if (pageId != rank.pageId) return false;
			return Math.abs(rank.value - value) < 0.001;
		}
	}

	public static class RankKeyFunction implements Function<Rank, Long> {
		@Override
		public Long apply(Rank rank) {
			return rank.pageId;
		}
	}

	public static class RankAccumulator {
		@Serialize
		public long pageId;
		@Serialize
		public double accumulatedRank;

		@SuppressWarnings("unused")
		public RankAccumulator() {
		}

		public RankAccumulator(long pageId) {
			this.pageId = pageId;
		}

		@Override
		public String toString() {
			return "RankAccumulator{pageId=" + pageId + ", accumulatedRank=" + accumulatedRank + '}';
		}
	}

	public static class RankAccumulatorKeyFunction implements Function<RankAccumulator, Long> {
		@Override
		public Long apply(RankAccumulator rankAccumulator) {
			return rankAccumulator.pageId;
		}
	}

	private static class RankAccumulatorReducer extends ReducerToResult<Long, Rank, Rank, RankAccumulator> {
		@Override
		public RankAccumulator createAccumulator(Long pageId) {
			return new RankAccumulator(pageId);
		}

		@Override
		public RankAccumulator accumulate(RankAccumulator accumulator, Rank value) {
			accumulator.accumulatedRank += value.value;
			return accumulator;
		}

		@Override
		public RankAccumulator combine(RankAccumulator accumulator, RankAccumulator anotherAccumulator) {
			accumulator.accumulatedRank += anotherAccumulator.accumulatedRank;
			return accumulator;
		}

		@Override
		public Rank produceResult(RankAccumulator accumulator) {
			return new Rank(accumulator.pageId, accumulator.accumulatedRank);
		}
	}

	public static class LongComparator implements Comparator<Long> {
		@Override
		public int compare(Long l1, Long l2) {
			return l1.compareTo(l2);
		}
	}

	public static class PageToRankFunction implements Function<Page, Rank> {
		@Override
		public Rank apply(Page page) {
			return new Rank(page.pageId, 1.0);
		}
	}

	public static class PageRankJoiner extends InnerJoiner<Long, Page, Rank, Rank> {
		@Override
		public void onInnerJoin(Long key, Page page, Rank rank, StreamDataAcceptor<Rank> output) {
			page.disperse(rank, output);
		}
	}

	private static SortedDataset<Long, Rank> pageRankIteration(SortedDataset<Long, Page> pages, SortedDataset<Long, Rank> ranks) {
		Dataset<Rank> updates = join(pages, ranks, new PageRankJoiner(), Rank.class, new RankKeyFunction());

		Dataset<Rank> newRanks = sortReduceRepartitionReduce(updates, new RankAccumulatorReducer(),
				Long.class, new RankKeyFunction(), new LongComparator(),
				RankAccumulator.class, new RankAccumulatorKeyFunction(),
				Rank.class);

		return castToSorted(newRanks, Long.class, new RankKeyFunction(), new LongComparator());
	}

	private static SortedDataset<Long, Rank> pageRank(SortedDataset<Long, Page> pages) {
		SortedDataset<Long, Rank> ranks = castToSorted(map(pages, new PageToRankFunction(), Rank.class),
				Long.class, new RankKeyFunction(), new LongComparator());

		for (int i = 0; i < 10; i++) {
			ranks = pageRankIteration(pages, ranks);
		}

		return ranks;
	}

	private Module createModule(Partition... partitions) throws Exception {
		return createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), List.of(partitions))
				.install(createSerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();
	}

	public DataflowServer launchServer(InetSocketAddress address, Object items, Object result) throws Exception {
		Injector env = Injector.of(ModuleBuilder.create()
				.install(createModule())
				.bind(datasetId("items")).toInstance(items)
				.bind(datasetId("result")).toInstance(result)
				.build());
		DataflowServer server = env.getInstance(DataflowServer.class).withListenAddress(address);
		server.listen();
		return server;
	}

	private static Iterable<Page> generatePages(int number) {
		return () -> new Iterator<>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < number;
			}

			@Override
			public Page next() {
				long[] links = new long[ThreadLocalRandom.current().nextInt(Math.min(100, number / 3))];
				for (int j = 0; j < links.length; j++) {
					links[j] = ThreadLocalRandom.current().nextInt(number);
				}
				return new Page(i++, links);
			}
		};
	}

	@SuppressWarnings("unused") // For manual run
	public void launchServers() throws Exception {
		launchServer(address1, generatePages(100000), (Consumer<Rank>) $ -> {});
		launchServer(address2, generatePages(90000), (Consumer<Rank>) $ -> {});
		await();
	}

	@SuppressWarnings("unused") // For manual run
	public void postPageRankTask() throws Exception {
		SortedDataset<Long, Page> sorted = sortedDatasetOfId("items", Page.class, Long.class, new PageKeyFunction(), new LongComparator());
		SortedDataset<Long, Page> repartitioned = repartitionSort(sorted);
		SortedDataset<Long, Rank> pageRanks = pageRank(repartitioned);

		Injector env = Injector.of(createModule(new Partition(address1), new Partition(address2)));
		DataflowGraph graph = env.getInstance(DataflowGraph.class);
		consumerOfId(pageRanks, "result").channels(DataflowContext.of(graph));

		await(graph.execute());
	}

	@SuppressWarnings("unused") // For manual run
	public void runDebugServer() throws Exception {
		Injector env = Injector.of(createModule(new Partition(address1), new Partition(address2)));
		env.getInstance(AsyncHttpServer.class).withListenPort(8080).listen();
		await();
	}

	@Test
	public void test() throws Exception {
		Module common = createModule(new Partition(address1), new Partition(address2));

		StreamConsumerToList<Rank> result1 = StreamConsumerToList.create();
		DataflowServer server1 = launchServer(address1, List.of(
				new Page(1, new long[]{1, 2, 3}),
				new Page(3, new long[]{1})), result1);

		StreamConsumerToList<Rank> result2 = StreamConsumerToList.create();
		DataflowServer server2 = launchServer(address2, List.of(new Page(2, new long[]{1})), result2);

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, Page> pages = repartitionSort(sortedDatasetOfId("items",
				Page.class, Long.class, new PageKeyFunction(), new LongComparator()));

		SortedDataset<Long, Rank> pageRanks = pageRank(pages);

		DatasetConsumerOfId<Rank> consumerNode = consumerOfId(pageRanks, "result");

		consumerNode.channels(DataflowContext.of(graph));

		await(graph.execute()
				.whenComplete(assertCompleteFn($ -> {
					server1.close();
					server2.close();
				})));

		List<Rank> result = new ArrayList<>();
		result.addAll(result1.getList());
		result.addAll(result2.getList());
		result.sort(Comparator.comparingLong(rank -> rank.pageId));

		assertEquals(List.of(new Rank(1, 1.7861), new Rank(2, 0.6069), new Rank(3, 0.6069)), result);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Module createSerializersModule() {
		return ModuleBuilder.create()
				.bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to(() -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();

					serializer.setSubtypeCodec(PageKeyFunction.class, ofObject(PageKeyFunction::new));
					serializer.setSubtypeCodec(RankKeyFunction.class, ofObject(RankKeyFunction::new));
					serializer.setSubtypeCodec(RankAccumulatorKeyFunction.class, ofObject(RankAccumulatorKeyFunction::new));
					serializer.setSubtypeCodec(PageToRankFunction.class, ofObject(PageToRankFunction::new));

					return serializer;
				})
				.bind(new Key<BinarySerializer<Comparator<?>>>() {}).toInstance(ofObject(LongComparator::new))
				.bind(new Key<BinarySerializer<ReducerToResult>>() {}).toInstance(ofObject(RankAccumulatorReducer::new))
				.bind(new Key<BinarySerializer<Joiner<?, ?, ?, ?>>>() {}).toInstance(ofObject(PageRankJoiner::new))
				.bind(new Key<BinarySerializer<Reducer<?, ?, ?, ?>>>() {}).to((mergeReducerSerializer, inputToAccumulatorSerializer, accumulatorToOutputSerializer) -> {
							FunctionSubtypeSerializer<Reducer> serializer = FunctionSubtypeSerializer.create();

							serializer.setSubtypeCodec(MergeReducer.class, mergeReducerSerializer);
							serializer.setSubtypeCodec(InputToAccumulator.class, inputToAccumulatorSerializer);
							serializer.setSubtypeCodec(AccumulatorToOutput.class, accumulatorToOutputSerializer);

							return (BinarySerializer<Reducer<?, ?, ?, ?>>) (BinarySerializer) serializer;
						},
						new Key<BinarySerializer<MergeReducer>>() {},
						new Key<BinarySerializer<InputToAccumulator>>() {},
						new Key<BinarySerializer<AccumulatorToOutput>>() {})
				.build();
	}

}
