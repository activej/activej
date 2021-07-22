package io.activej.cube;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.multilog.Multilog;
import io.activej.multilog.MultilogImpl;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeMeasureRemovalTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static final FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	private Eventloop eventloop;
	private Executor executor;
	private DefiningClassLoader classLoader;
	private DataSource dataSource;
	private AggregationChunkStorage<Long> aggregationChunkStorage;
	private Multilog<LogItem> multilog;
	private Path aggregationsDir;
	private Path logsDir;

	@Before
	public void before() throws IOException, SQLException {
		aggregationsDir = temporaryFolder.newFolder().toPath();
		logsDir = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.getCurrentEventloop();
		executor = Executors.newCachedThreadPool();
		classLoader = DefiningClassLoader.create();
		dataSource = dataSource("test.properties");
		LocalActiveFs fs = LocalActiveFs.create(eventloop, executor, aggregationsDir);
		await(fs.start());
		aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), FRAME_FORMAT, fs);
		BinarySerializer<LogItem> serializer = SerializerBuilder.create(classLoader).build(LogItem.class);
		LocalActiveFs localFs = LocalActiveFs.create(eventloop, executor, logsDir);
		await(localFs.start());
		multilog = MultilogImpl.create(eventloop,
				localFs,
				LZ4FrameFormat.create(),
				serializer,
				NAME_PARTITION_REMAINDER_SEQ);
	}

//	@Test
//	public void test() throws Exception {
//		LocalActiveFs fs = LocalActiveFs.create(eventloop, executor, aggregationsDir);
//		await(fs.start());
//		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), FRAME_FORMAT, fs);
//		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("date", ofLocalDate())
//				.withDimension("advertiser", ofInt())
//				.withDimension("campaign", ofInt())
//				.withDimension("banner", ofInt())
//				.withMeasure("impressions", sum(ofLong()))
//				.withMeasure("clicks", sum(ofLong()))
//				.withMeasure("conversions", sum(ofLong()))
//				.withMeasure("revenue", sum(ofDouble()))
//				.withAggregation(id("detailed")
//						.withDimensions("date", "advertiser", "campaign", "banner")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"))
//				.withAggregation(id("date")
//						.withDimensions("date")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"))
//				.withAggregation(id("advertiser")
//						.withDimensions("advertiser")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"))
//				.withRelation("campaign", "advertiser")
//				.withRelation("banner", "campaign");
//
//		DataSource dataSource = dataSource("test.properties");
//		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
//		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//				otSystem, LogDiffCodec.create(CubeDiffCodec.create(cube)));
//		initializeRepository(repository);
//
//		LocalActiveFs localFs = LocalActiveFs.create(eventloop, executor, logsDir);
//		await(localFs.start());
//		Multilog<LogItem> multilog = MultilogImpl.create(eventloop,
//				localFs,
//				LZ4FrameFormat.create(),
//				SerializerBuilder.create(classLoader).build(LogItem.class),
//				NAME_PARTITION_REMAINDER_SEQ);
//
//		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
//		OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node = OTUplinkImpl.create(repository, otSystem);
//		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState);
//
//		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop, multilog,
//				cube.logStreamConsumer(LogItem.class), "testlog", asList("partitionA"), cubeDiffLogOTState);
//
//		// checkout first (root) revision
//		await(logCubeStateManager.checkout());
//
//		// Save and aggregate logs
//		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
//		await(StreamSupplier.ofIterable(listOfRandomLogItems1).streamTo(
//				StreamConsumer.ofPromise(multilog.write("partitionA"))));
//
//		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager1 = logCubeStateManager;
//		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager1, logOTProcessor);
//
//		List<AggregationChunk> chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
//		assertEquals(1, chunks.size());
//		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
//
//		// Initialize cube with new structure (removed measure)
//		cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("date", ofLocalDate())
//				.withDimension("advertiser", ofInt())
//				.withDimension("campaign", ofInt())
//				.withDimension("banner", ofInt())
//				.withMeasure("impressions", sum(ofLong()))
//				.withMeasure("clicks", sum(ofLong()))
//				.withMeasure("conversions", sum(ofLong()))
//				.withMeasure("revenue", sum(ofDouble()))
//				.withAggregation(id("detailed")
//						.withDimensions("date", "advertiser", "campaign", "banner")
//						.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
//				.withAggregation(id("date")
//						.withDimensions("date")
//						.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
//				.withAggregation(id("advertiser")
//						.withDimensions("advertiser")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"))
//				.withRelation("campaign", "advertiser")
//				.withRelation("banner", "campaign");
//
//		LogOTState<CubeDiff> cubeDiffLogOTState1 = LogOTState.create(cube);
//		logCubeStateManager = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState1);
//
//		logOTProcessor = LogOTProcessor.create(eventloop, multilog, cube.logStreamConsumer(LogItem.class),
//				"testlog", asList("partitionA"), cubeDiffLogOTState1);
//
//		await(logCubeStateManager.checkout());
//
//		// Save and aggregate logs
//		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
//		await(StreamSupplier.ofIterable(listOfRandomLogItems2).streamTo(
//				StreamConsumer.ofPromise(multilog.write("partitionA"))));
//
//		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager = logCubeStateManager;
//		LogOTProcessor<LogItem, CubeDiff> finalLogOTProcessor = logOTProcessor;
//		await(finalLogCubeStateManager.sync());
//		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager, finalLogOTProcessor);
//
//		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
//		assertEquals(2, chunks.size());
//		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
//		assertFalse(chunks.get(1).getMeasures().contains("revenue"));
//
//		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
//		assertEquals(2, chunks.size());
//		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
//		assertTrue(chunks.get(1).getMeasures().contains("revenue"));
//
//		// Aggregate manually
//		Map<Integer, Long> map = Stream.concat(listOfRandomLogItems1.stream(), listOfRandomLogItems2.stream())
//				.collect(groupingBy(o -> o.date, reducing(0L, o -> o.clicks, Long::sum)));
//
//		StreamConsumerToList<LogItem> queryResultConsumer2 = StreamConsumerToList.create();
//		await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, classLoader).streamTo(
//				queryResultConsumer2));
//
//		// Check query results
//		List<LogItem> queryResult2 = queryResultConsumer2.getList();
//		for (LogItem logItem : queryResult2) {
//			assertEquals(logItem.clicks, map.remove(logItem.date).longValue());
//		}
//		assertTrue(map.isEmpty());
//
//		// Consolidate
//		CubeDiff consolidatingCubeDiff = await(cube.consolidate(Aggregation::consolidateHotSegment));
//		await(aggregationChunkStorage.finish(consolidatingCubeDiff.addedChunks().map(id -> (long) id).collect(toSet())));
//		assertFalse(consolidatingCubeDiff.isEmpty());
//
//		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
//		await(logCubeStateManager.sync());
//
//		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
//		assertEquals(1, chunks.size());
//		assertFalse(chunks.get(0).getMeasures().contains("revenue"));
//
//		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
//		assertEquals(1, chunks.size());
//		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
//
//		// Query
//		StreamConsumerToList<LogItem> queryResultConsumer3 = StreamConsumerToList.create();
//		await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, DefiningClassLoader.create(classLoader))
//				.streamTo(queryResultConsumer3));
//		List<LogItem> queryResult3 = queryResultConsumer3.getList();
//
//		// Check that query results before and after consolidation match
//		assertEquals(queryResult2.size(), queryResult3.size());
//		for (int i = 0; i < queryResult2.size(); ++i) {
//			assertEquals(queryResult2.get(i).date, queryResult3.get(i).date);
//			assertEquals(queryResult2.get(i).clicks, queryResult3.get(i).clicks);
//		}
//	}

//	@Test
//	public void testNewUnknownMeasureInAggregationDiffOnDeserialization() throws Throwable {
//		{
//			Cube cube1 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//					.withDimension("date", ofLocalDate())
//					.withMeasure("impressions", sum(ofLong()))
//					.withMeasure("clicks", sum(ofLong()))
//					.withMeasure("conversions", sum(ofLong()))
//					.withAggregation(id("date")
//							.withDimensions("date")
//							.withMeasures("impressions", "clicks", "conversions"));
//
//			LogDiffCodec<CubeDiff> diffCodec1 = LogDiffCodec.create(CubeDiffCodec.create(cube1));
//			OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
//			OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//					otSystem, diffCodec1);
//			initializeRepository(repository);
//
//			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
//			OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node = OTUplinkImpl.create(repository, otSystem);
//			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState);
//
//			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);
//			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
//					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);
//
//			await(logCubeStateManager1.checkout());
//
//			await(StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
//					StreamConsumer.ofPromise(multilog.write("partitionA"))));
//
//			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
//		}
//
//		// Initialize cube with new structure (remove "clicks" from cube configuration)
//		Cube cube2 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("date", ofLocalDate())
//				.withMeasure("impressions", sum(ofLong()))
//				.withAggregation(id("date")
//						.withDimensions("date")
//						.withMeasures("impressions"));
//
//		LogDiffCodec<CubeDiff> diffCodec2 = LogDiffCodec.create(CubeDiffCodec.create(cube2));
//		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
//		OTRepositoryMySql<LogDiff<CubeDiff>> otSourceSql2 = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//				otSystem, diffCodec2);
//		otSourceSql2.initialize();
//
//		Throwable exception = awaitException(otSourceSql2.getHeads());
//		assertThat(exception, instanceOf(MalformedDataException.class));
//		assertEquals("Unknown fields: [clicks, conversions]", exception.getMessage());
//	}

//	@Test
//	public void testUnknownAggregation() throws Throwable {
//		{
//			Cube cube1 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//					.withDimension("date", ofLocalDate())
//					.withMeasure("impressions", sum(ofLong()))
//					.withMeasure("clicks", sum(ofLong()))
//					.withAggregation(id("date")
//							.withDimensions("date")
//							.withMeasures("impressions", "clicks"))
//					.withAggregation(id("impressionsAggregation")
//							.withDimensions("date")
//							.withMeasures("impressions"))
//					.withAggregation(id("otherAggregation")
//							.withDimensions("date")
//							.withMeasures("clicks"));
//
//			LogDiffCodec<CubeDiff> diffCodec1 = LogDiffCodec.create(CubeDiffCodec.create(cube1));
//			OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
//			OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//					otSystem, diffCodec1);
//			initializeRepository(repository);
//
//			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
//			OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node = OTUplinkImpl.create(repository, otSystem);
//			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState);
//
//			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);
//
//			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(eventloop,
//					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);
//
//			await(logCubeStateManager1.checkout());
//
//			await(StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
//					StreamConsumer.ofPromise(multilog.write("partitionA"))));
//
//			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
//		}
//
//		// Initialize cube with new structure (remove "impressions" aggregation from cube configuration)
//		Cube cube2 = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("date", ofLocalDate())
//				.withMeasure("impressions", sum(ofLong()))
//				.withMeasure("clicks", sum(ofLong()))
//				.withAggregation(id("date")
//						.withDimensions("date")
//						.withMeasures("impressions", "clicks"));
//
//		LogDiffCodec<CubeDiff> diffCodec2 = LogDiffCodec.create(CubeDiffCodec.create(cube2));
//		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
//		OTRepositoryMySql<LogDiff<CubeDiff>> otSourceSql2 = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//				otSystem, diffCodec2);
//		otSourceSql2.initialize();
//
//		Throwable exception = awaitException(otSourceSql2.getHeads());
//		assertThat(exception, instanceOf(MalformedDataException.class));
//		assertEquals("Unknown aggregation: impressionsAggregation", exception.getMessage());
//	}
}
