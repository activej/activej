package io.activej.dataflow.stream;

import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dataset.impl.DatasetConsumerOfId;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.node.Node_Sort.StreamSorterStorageFactory;
import io.activej.datastream.StreamConsumer_ToList;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner_LeftInner;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.http.HttpServer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

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

import static io.activej.dataflow.codec.SubtypeImpl.subtype;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.graph.StreamSchemas.simple;
import static io.activej.dataflow.helper.StreamSorterStorage_MergeStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.stream.DataflowTest.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

public class PageRankTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

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

	public static class LeftJoiner_PageRank extends LeftJoiner_LeftInner<Long, Page, Rank, Rank> {
		@Override
		public void onInnerJoin(Long key, Page page, Rank rank, StreamDataAcceptor<Rank> output) {
			page.disperse(rank, output);
		}
	}

	private static SortedDataset<Long, Rank> pageRankIteration(SortedDataset<Long, Page> pages, SortedDataset<Long, Rank> ranks) {
		Dataset<Rank> updates = join(pages, ranks, new LeftJoiner_PageRank(), simple(Rank.class), new RankKeyFunction());

		Dataset<Rank> newRanks = sortReduceRepartitionReduce(updates, new RankAccumulatorReducer(),
				Long.class, new RankKeyFunction(), new LongComparator(),
				simple(RankAccumulator.class), new RankAccumulatorKeyFunction(),
				simple(Rank.class));

		return castToSorted(newRanks, Long.class, new RankKeyFunction(), new LongComparator());
	}

	private static SortedDataset<Long, Rank> pageRank(SortedDataset<Long, Page> pages) {
		SortedDataset<Long, Rank> ranks = castToSorted(map(pages, new PageToRankFunction(), simple(Rank.class)),
				Long.class, new RankKeyFunction(), new LongComparator());

		for (int i = 0; i < 10; i++) {
			ranks = pageRankIteration(pages, ranks);
		}

		return ranks;
	}

	private Module createModule(Partition... partitions) {
		return createCommon(List.of(partitions))
				.install(createSerializersModule())
				.build();
	}

	public DataflowServer launchServer(InetSocketAddress address, Object items, Object result) throws Exception {
		Injector env = Injector.of(ModuleBuilder.create()
				.install(createCommonServer(createModule(), executor, sortingExecutor))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.install(DatasetIdModule.create())
				.bind(datasetId("items")).toInstance(items)
				.bind(datasetId("result")).toInstance(result)
				.bind(Integer.class, "dataflowPort").toInstance(address.getPort())
				.build());
		DataflowServer server = env.getInstance(DataflowServer.class);
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

	InetSocketAddress addressForDebug1 = new InetSocketAddress(9001);
	InetSocketAddress addressForDebug2 = new InetSocketAddress(9002);

	@Ignore("For manual run")
	@Test
	public void launchServers() throws Exception {
		launchServer(addressForDebug1, generatePages(100000), (Consumer<Rank>) $ -> {});
		launchServer(addressForDebug2, generatePages(90000), (Consumer<Rank>) $ -> {});
		await();
	}

	@Ignore("For manual run")
	@Test
	public void runDebugServer() throws Exception {
		Injector env = Injector.of(
				createCommonServer(
						createModule(new Partition(addressForDebug1), new Partition(addressForDebug2)),
						executor, sortingExecutor),
				ModuleBuilder.create()
						.bind(Integer.class, "debugPort").toInstance(8080)
						.build()
		);
		env.getInstance(HttpServer.class).listen();
		await();
	}

	@Ignore("For manual run")
	@Test
	public void postPageRankTask() {
		SortedDataset<Long, Page> sorted = sortedDatasetOfId("items", simple(Page.class), Long.class, new PageKeyFunction(), new LongComparator());
		SortedDataset<Long, Page> repartitioned = repartitionSort(sorted);
		SortedDataset<Long, Rank> pageRanks = pageRank(repartitioned);

		Injector env = Injector.of(createCommonClient(createModule(new Partition(addressForDebug1), new Partition(addressForDebug2))));
		DataflowGraph graph = env.getInstance(DataflowGraph.class);
		consumerOfId(pageRanks, "result").channels(DataflowContext.of(graph));

		await(graph.execute());
	}

	@Test
	public void test() throws Exception {
		Module common = createModule(new Partition(address1), new Partition(address2));

		StreamConsumer_ToList<Rank> result1 = StreamConsumer_ToList.create();
		DataflowServer server1 = launchServer(address1, List.of(
				new Page(1, new long[]{1, 2, 3}),
				new Page(3, new long[]{1})), result1);

		StreamConsumer_ToList<Rank> result2 = StreamConsumer_ToList.create();
		DataflowServer server2 = launchServer(address2, List.of(new Page(2, new long[]{1})), result2);

		DataflowGraph graph = Injector.of(createCommonClient(common)).getInstance(DataflowGraph.class);

		SortedDataset<Long, Page> pages = repartitionSort(sortedDatasetOfId("items",
				simple(Page.class), Long.class, new PageKeyFunction(), new LongComparator()));

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

	private static Module createSerializersModule() {
		return ModuleBuilder.create()

				.bind(new Key<StreamCodec<PageKeyFunction>>(subtype(0)) {}).toInstance(StreamCodecs.singleton(new PageKeyFunction()))
				.bind(new Key<StreamCodec<RankKeyFunction>>(subtype(1)) {}).toInstance(StreamCodecs.singleton(new RankKeyFunction()))
				.bind(new Key<StreamCodec<RankAccumulatorKeyFunction>>(subtype(2)) {}).toInstance(StreamCodecs.singleton(new RankAccumulatorKeyFunction()))
				.bind(new Key<StreamCodec<PageToRankFunction>>(subtype(3)) {}).toInstance(StreamCodecs.singleton(new PageToRankFunction()))

				.bind(new Key<StreamCodec<Comparator<?>>>() {}).toInstance(StreamCodecs.singleton(new LongComparator()))

				.bind(new Key<StreamCodec<LeftJoiner<?, ?, ?, ?>>>() {}).toInstance(StreamCodecs.singleton(new LeftJoiner_PageRank()))

				.bind(new Key<StreamCodec<ReducerToResult<?, ?, ?, ?>>>() {}).toInstance(StreamCodecs.singleton(new RankAccumulatorReducer()))

				.build();
	}

}
