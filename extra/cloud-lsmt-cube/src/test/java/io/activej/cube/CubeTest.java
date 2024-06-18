package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.CubeConsolidator.ConsolidationStrategy;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.aggregation.predicate.AggregationPredicates;
import io.activej.cube.bean.*;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.dns.DnsClient;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.fs.FileSystem;
import io.activej.fs.http.FileSystemServlet;
import io.activej.fs.http.HttpClientFileSystem;
import io.activej.http.HttpClient;
import io.activej.http.HttpServer;
import io.activej.http.IHttpClient;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.activej.codegen.DefiningClassLoader.create;
import static io.activej.common.Utils.toLinkedHashMap;
import static io.activej.cube.CubeConsolidator.ConsolidationStrategy.hotSegment;
import static io.activej.cube.CubeConsolidator.ConsolidationStrategy.withConsolidationPredicate;
import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.stubChunkIdGenerator;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.*;
import static io.activej.cube.aggregation.util.Utils.materializeProtoDiff;
import static io.activej.http.HttpUtils.inetAddress;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class CubeTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static final FrameFormat FRAME_FORMAT = FrameFormats.lz4();

	private final DefiningClassLoader classLoader = create();
	private final Executor executor = newSingleThreadExecutor();

	private IAggregationChunkStorage chunkStorage;
	private CubeReporting cubeReporting;
	private int listenPort;

	@Before
	public void setUp() throws Exception {
		listenPort = getFreePort();
		FileSystem fs = FileSystem.create(getCurrentReactor(), executor, temporaryFolder.newFolder().toPath());
		await(fs.start());
		chunkStorage = AggregationChunkStorage.create(getCurrentReactor(), stubChunkIdGenerator(), FRAME_FORMAT, fs);
		CubeStructure cubeStructure = newCubeStructure();
		cubeReporting = createCubeReporting(cubeStructure, executor, classLoader, chunkStorage);
	}

	private static CubeStructure newCubeStructure() {
		return CubeStructure.builder()
			.withDimension("key1", ofInt())
			.withDimension("key2", ofInt())
			.withMeasure("metric1", sum(ofLong()))
			.withMeasure("metric2", sum(ofLong()))
			.withMeasure("metric3", sum(ofLong()))
			.withAggregation(id("detailedAggregation")
				.withDimensions("key1", "key2")
				.withMeasures("metric1", "metric2", "metric3"))
			.build();
	}

	private static CubeStructure newSophisticatedCubeStructure() {
		return CubeStructure.builder()
			.withDimension("key1", ofInt())
			.withDimension("key2", ofInt())
			.withDimension("key3", ofInt())
			.withDimension("key4", ofInt())
			.withDimension("key5", ofInt())
			.withMeasure("metric1", sum(ofLong()))
			.withMeasure("metric2", sum(ofLong()))
			.withMeasure("metric3", sum(ofLong()))
			.withAggregation(id("detailedAggregation")
				.withDimensions("key1", "key2", "key3", "key4", "key5")
				.withMeasures("metric1", "metric2", "metric3"))
			.build();
	}

	private CubeReporting createCubeReporting(CubeStructure cubeStructure, Executor executor, DefiningClassLoader classLoader, IAggregationChunkStorage aggregationChunkStorage) {
		CubeExecutor cubeExecutor = CubeExecutor.create(getCurrentReactor(), cubeStructure, executor, classLoader, aggregationChunkStorage);
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = TestUtils.stubStateManager(cubeStructure);
		return CubeReporting.create(stateManager, cubeStructure, cubeExecutor);
	}

	@SuppressWarnings("unchecked")
	private static <T> Promise<Void> consume(CubeReporting cubeReporting, IAggregationChunkStorage chunkStorage, T item, T... items) {
		return StreamSuppliers.concat(StreamSuppliers.ofValue(item), StreamSuppliers.ofValues(items))
			.streamTo(cubeReporting.getExecutor().consume(((Class<T>) item.getClass())))
			.then(cubeDiff -> chunkStorage.finish(cubeDiff.addedProtoChunks().collect(toSet()))
				.then(chunkIds -> cubeReporting.getStateManager().push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(cubeDiff, chunkIds))))));
	}

	@Test
	public void testQuery1() {
		List<DataItemResult> expected = List.of(new DataItemResult(1, 3, 10, 30, 20));

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20))
		);
		List<DataItemResult> list = await(cubeReporting.queryRawStream(
				List.of("key1", "key2"),
				List.of("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, classLoader)
			.toList());

		assertEquals(expected, list);
	}

	private HttpServer startServer(Executor executor, Path serverStorage) throws IOException {
		NioReactor reactor = getCurrentReactor();
		FileSystem fs = FileSystem.create(reactor, executor, serverStorage);
		await(fs.start());
		HttpServer server = HttpServer.builder(reactor, FileSystemServlet.create(reactor, fs))
			.withListenPort(listenPort)
			.build();
		server.listen();
		return server;
	}

	@Test
	public void testRemoteFileSystemAggregationStorage() throws Exception {
		Path serverStorage = temporaryFolder.newFolder("storage").toPath();
		HttpServer server1 = startServer(executor, serverStorage);
		NioReactor reactor = getCurrentReactor();
		DnsClient dnsClient = DnsClient.create(reactor, inetAddress("8.8.8.8"));
		IHttpClient httpClient = HttpClient.create(reactor, dnsClient);
		HttpClientFileSystem storage = HttpClientFileSystem.create(reactor, "http://localhost:" + listenPort, httpClient);
		IAggregationChunkStorage chunkStorage = AggregationChunkStorage.create(reactor, stubChunkIdGenerator(), FRAME_FORMAT, storage);
		cubeReporting = createCubeReporting(cubeReporting.getStructure(), executor, classLoader, chunkStorage);

		List<DataItemResult> expected = List.of(new DataItemResult(1, 3, 10, 30, 20));

		await(
			Promises.all(consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
					consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.whenComplete(server1::close)
		);
		HttpServer server2 = startServer(executor, serverStorage);

		List<DataItemResult> list = await(cubeReporting.queryRawStream(
				List.of("key1", "key2"), List.of("metric1", "metric2", "metric3"),
				and(eq("key1", 1), eq("key2", 3)),
				DataItemResult.class, classLoader)
			.toList()
			.whenComplete(server2::close));

		assertEquals(expected, list);
	}

	@Test
	public void testOrdering() {
		List<DataItemResult> expected = List.of(
			new DataItemResult(1, 2, 30, 37, 42), // metric2 =  37
			new DataItemResult(1, 3, 44, 43, 5),  // metric2 =  43
			new DataItemResult(1, 4, 23, 161, 42) // metric2 = 161
		);

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 30, 25), new DataItem1(1, 3, 40, 10), new DataItem1(1, 4, 23, 48), new DataItem1(1, 3, 4, 18)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 15, 5), new DataItem2(1, 4, 55, 20), new DataItem2(1, 2, 12, 42), new DataItem2(1, 4, 58, 22))
		);
		List<DataItemResult> list = await(cubeReporting.queryRawStream(
			List.of("key1", "key2"),
			List.of("metric1", "metric2", "metric3"),
			alwaysTrue(),
			DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testMultipleOrdering() {
		List<DataItemResult> expected = List.of(
			new DataItemResult(1, 3, 30, 25, 0),  // metric1 = 30, metric2 = 25
			new DataItemResult(1, 4, 40, 10, 0),  // metric1 = 40, metric2 = 10
			new DataItemResult(1, 5, 23, 48, 0),  // metric1 = 23, metric2 = 48
			new DataItemResult(1, 6, 4, 18, 0),   // metric1 =  4, metric2 = 18
			new DataItemResult(1, 7, 0, 15, 5),   // metric1 =  0, metric2 = 15
			new DataItemResult(1, 8, 0, 55, 20),  // metric1 =  0, metric2 = 55
			new DataItemResult(1, 9, 0, 12, 42),  // metric1 =  0, metric2 = 12
			new DataItemResult(1, 10, 0, 58, 22)  // metric1 =  0, metric2 = 58
		);

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10), new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20), new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22))
		);

		List<DataItemResult> list = await(cubeReporting.queryRawStream(
			List.of("key1", "key2"),
			List.of("metric1", "metric2", "metric3"),
			alwaysTrue(),
			DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testBetweenPredicate() {
		List<DataItemResult> expected = List.of(
			new DataItemResult(5, 77, 0, 88, 98),
			new DataItemResult(5, 99, 40, 36, 0),
			new DataItemResult(8, 42, 0, 33, 17)
		);

		await(
			consume(cubeReporting, chunkStorage,
				new DataItem1(14, 1, 30, 25), new DataItem1(13, 3, 40, 10), new DataItem1(9, 4, 23, 48), new DataItem1(6, 3, 4, 18),
				new DataItem1(10, 5, 22, 16), new DataItem1(20, 7, 13, 49), new DataItem1(15, 9, 11, 12), new DataItem1(5, 99, 40, 36)),
			consume(cubeReporting, chunkStorage, new DataItem1(1, 3, 30, 25), new DataItem1(1, 4, 40, 10), new DataItem1(1, 5, 23, 48), new DataItem1(1, 6, 4, 18)),
			consume(cubeReporting, chunkStorage,
				new DataItem2(9, 3, 15, 5), new DataItem2(11, 4, 55, 20), new DataItem2(17, 2, 12, 42), new DataItem2(11, 4, 58, 22),
				new DataItem2(19, 18, 22, 55), new DataItem2(7, 14, 28, 6), new DataItem2(8, 42, 33, 17), new DataItem2(5, 77, 88, 98)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 7, 15, 5), new DataItem2(1, 8, 55, 20), new DataItem2(1, 9, 12, 42), new DataItem2(1, 10, 58, 22))
		);

		List<DataItemResult> list = await(cubeReporting.queryRawStream(
			List.of("key1", "key2"),
			List.of("metric1", "metric2", "metric3"),
			and(between("key1", 5, 10), between("key2", 40, 1000)),
			DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testBetweenTransformation() {
		cubeReporting = createCubeReporting(newSophisticatedCubeStructure(), executor, classLoader, chunkStorage);

		List<DataItemResult3> expected = List.of(new DataItemResult3(5, 77, 50, 20, 56, 0, 88, 98));

		await(
			consume(cubeReporting, chunkStorage,
				new DataItem3(14, 1, 42, 25, 53, 30, 25), new DataItem3(13, 3, 49, 13, 50, 40, 10), new DataItem3(9, 4, 59, 17, 79, 23, 48),
				new DataItem3(6, 3, 30, 20, 63, 4, 18), new DataItem3(10, 5, 33, 21, 69, 22, 16), new DataItem3(20, 7, 39, 29, 65, 13, 49),
				new DataItem3(15, 9, 57, 26, 59, 11, 12), new DataItem3(5, 99, 35, 27, 76, 40, 36)),
			consume(cubeReporting, chunkStorage,
				new DataItem4(9, 3, 41, 11, 65, 15, 5), new DataItem4(11, 4, 38, 10, 68, 55, 20), new DataItem4(17, 2, 40, 15, 52, 12, 42),
				new DataItem4(11, 4, 47, 22, 60, 58, 22), new DataItem4(19, 18, 52, 24, 80, 22, 55), new DataItem4(7, 14, 31, 14, 73, 28, 6),
				new DataItem4(8, 42, 46, 19, 75, 33, 17), new DataItem4(5, 77, 50, 20, 56, 88, 98))
		);

		List<DataItemResult3> list = await(cubeReporting.queryRawStream(
			List.of("key1", "key2", "key3", "key4", "key5"),
			List.of("metric1", "metric2", "metric3"),
			and(eq("key1", 5), between("key2", 75, 99), between("key3", 35, 50), eq("key4", 20), eq("key5", 56)),
			DataItemResult3.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testGrouping() {
		List<DataItemResult2> expected = List.of(
			new DataItemResult2(1, 150, 230, 75),
			new DataItemResult2(2, 25, 45, 0),
			new DataItemResult2(3, 10, 40, 10),
			new DataItemResult2(4, 5, 45, 20));

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20), new DataItem1(1, 2, 15, 25),
				new DataItem1(1, 1, 95, 85), new DataItem1(2, 1, 55, 65), new DataItem1(1, 4, 5, 35)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 20, 10), new DataItem2(1, 4, 10, 20), new DataItem2(1, 1, 80, 75))
		);
		// SELECT key1, SUM(metric1), SUM(metric2), SUM(metric3) FROM detailedAggregation WHERE key1 = 1 AND key2 = 3 GROUP BY key1

		List<DataItemResult2> list = await(cubeReporting.queryRawStream(List.of("key2"), List.of("metric1", "metric2", "metric3"),
			alwaysTrue(),
			DataItemResult2.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testQuery2() {
		List<DataItemResult> expected = List.of(new DataItemResult(1, 3, 10, 30, 20));

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200))
		);

		List<DataItemResult> list = await(cubeReporting.queryRawStream(List.of("key1", "key2"), List.of("metric1", "metric2", "metric3"),
			and(eq("key1", 1), eq("key2", 3)),
			DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testConsolidate() {
		List<DataItemResult> expected = List.of(new DataItemResult(1, 4, 0, 30, 60));
		CubeConsolidator cubeConsolidator = CubeConsolidator.create(cubeReporting.getStateManager(), cubeReporting.getExecutor());

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 2, 10, 20), new DataItem2(1, 4, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 4, 10, 20), new DataItem2(1, 5, 100, 200))
		);

		List<String> aggregationIds = List.copyOf(cubeReporting.getStructure().getAggregationIds());
		CubeDiff diff = await(cubeConsolidator.consolidate(aggregationIds, hotSegment()));
		assertFalse(diff.isEmpty());

		diff = await(cubeConsolidator.consolidate(aggregationIds, hotSegment()));
		assertTrue(diff.isEmpty());

		List<DataItemResult> list = await(cubeReporting.queryRawStream(
			List.of("key1", "key2"),
			List.of("metric1", "metric2", "metric3"),
			and(eq("key1", 1), eq("key2", 4)),
			DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testConsolidationPredicate() {
		List<DataItemResult> expected = List.of(new DataItemResult(1, 0, 0, 100, 0));
		CubeConsolidator cubeConsolidator = CubeConsolidator.create(cubeReporting.getStateManager(), cubeReporting.getExecutor());

		await(
			consume(cubeReporting, chunkStorage, new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 25, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 2, 15, 20), new DataItem2(1, 4, 35, 20)),
			consume(cubeReporting, chunkStorage, new DataItem2(1, 4, 20, 20), new DataItem2(1, 5, 40, 200))
		);

		ConsolidationStrategy strategy = withConsolidationPredicate(hotSegment(), $ -> gt("metric2", 20L));

		List<String> aggregationIds = List.copyOf(cubeReporting.getStructure().getAggregationIds());
		CubeDiff diff = await(cubeConsolidator.consolidate(aggregationIds, strategy));
		assertFalse(diff.isEmpty());

		diff = await(cubeConsolidator.consolidate(aggregationIds, strategy));
		assertTrue(diff.isEmpty());

		List<DataItemResult> list = await(cubeReporting.queryRawStream(
			List.of("key1"),
			List.of("metric2"),
			alwaysTrue(),
			DataItemResult.class, classLoader
		).toList());

		assertEquals(expected, list);
	}

	@Test
	public void testAggregationPredicate() {
		AggregationPredicate predicate;
		AggregationPredicate query;
		AggregationPredicate intersection;

		predicate = AggregationPredicates.alwaysTrue();
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		predicate = AggregationPredicates.eq("dimensionA", 1);
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		predicate = AggregationPredicates.eq("dimensionA", 1);
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 2), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);

		predicate = AggregationPredicates.eq("dimensionA", 1);
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionA"), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);

		predicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionA", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);

		predicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.eq("dimensionX", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		predicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.and(AggregationPredicates.eq("dimensionX", 1), AggregationPredicates.eq("dimensionB", 2))).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		predicate = AggregationPredicates.has("dimensionX");
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("dimensionX", 1), AggregationPredicates.eq("dimensionB", 2)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		// betweens

		predicate = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 100, 200));
		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("date", 1)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("date", 150)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.eq("date", 250)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 110, 190)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertEquals(intersection, query);

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 10, 90)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);
		assertEquals(intersection, AggregationPredicates.alwaysFalse());

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 210, 290)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);
		assertEquals(intersection, AggregationPredicates.alwaysFalse());

		query = AggregationPredicates.and(AggregationPredicates.has("dimensionX"), AggregationPredicates.between("date", 10, 290)).simplify();
		intersection = AggregationPredicates.and(query, predicate).simplify();
		assertNotEquals(intersection, query);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknownDimensions() throws IOException {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Executor executor = newSingleThreadExecutor();

		FileSystem storage = FileSystem.create(getCurrentReactor(), executor, temporaryFolder.newFolder().toPath());
		await(storage.start());
		IAggregationChunkStorage chunkStorage = AggregationChunkStorage.create(getCurrentReactor(), stubChunkIdGenerator(), FRAME_FORMAT, storage);
		CubeStructure cubeStructure = newCubeStructure();
		CubeExecutor cubeExecutor = CubeExecutor.create(getCurrentReactor(), cubeStructure, executor, classLoader, chunkStorage);

		cubeExecutor.consume(DataItem1.class,
			Stream.of("unknownKey")
				.collect(toLinkedHashMap(identity())),
			Stream.of("metric1", "metric2", "metric3")
				.collect(toLinkedHashMap(identity())),
			AggregationPredicates.alwaysTrue());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnknownMeasure() throws IOException {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Executor executor = newSingleThreadExecutor();

		FileSystem storage = FileSystem.create(getCurrentReactor(), executor, temporaryFolder.newFolder().toPath());
		await(storage.start());
		IAggregationChunkStorage chunkStorage = AggregationChunkStorage.create(getCurrentReactor(), stubChunkIdGenerator(), FRAME_FORMAT, storage);
		CubeStructure cubeStructure = newCubeStructure();
		CubeExecutor cubeExecutor = CubeExecutor.create(getCurrentReactor(), cubeStructure, executor, classLoader, chunkStorage);

		cubeExecutor.consume(DataItem1.class,
			Stream.of("key1", "key2")
				.collect(toLinkedHashMap(identity())),
			Stream.of("UnknownMetric")
				.collect(toLinkedHashMap(identity())),
			AggregationPredicates.alwaysTrue());
	}

}
