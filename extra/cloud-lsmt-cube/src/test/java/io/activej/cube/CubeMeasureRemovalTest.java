package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.ILogDataConsumer;
import io.activej.etl.LogDiff;
import io.activej.etl.LogProcessor;
import io.activej.etl.LogState;
import io.activej.fs.FileSystem;
import io.activej.json.JsonValidationException;
import io.activej.multilog.IMultilog;
import io.activej.multilog.Multilog;
import io.activej.ot.StateManager;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.activej.cube.CubeConsolidator.ConsolidationStrategy.hotSegment;
import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.cube.TestUtils.stubChunkIdGenerator;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.*;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.alwaysTrue;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class CubeMeasureRemovalTest extends CubeTestBase {
	private static final FrameFormat FRAME_FORMAT = FrameFormats.lz4();

	private IAggregationChunkStorage aggregationChunkStorage;
	private IMultilog<LogItem> multilog;
	private Path aggregationsDir;
	private Path logsDir;

	@Before
	public void before() throws IOException {
		aggregationsDir = temporaryFolder.newFolder().toPath();
		logsDir = temporaryFolder.newFolder().toPath();

		FileSystem fs = FileSystem.create(reactor, EXECUTOR, aggregationsDir);
		await(fs.start());
		aggregationChunkStorage = AggregationChunkStorage.create(reactor, stubChunkIdGenerator(), FRAME_FORMAT, fs);
		BinarySerializer<LogItem> serializer = SerializerFactory.defaultInstance().create(CLASS_LOADER, LogItem.class);
		FileSystem fileSystem = FileSystem.create(reactor, EXECUTOR, logsDir);
		await(fileSystem.start());
		multilog = Multilog.create(reactor,
			fileSystem,
			FrameFormats.lz4(),
			serializer,
			NAME_PARTITION_REMAINDER_SEQ);
	}

	@Test
	public void test() throws Exception {
		FileSystem fs = FileSystem.create(reactor, EXECUTOR, aggregationsDir);
		await(fs.start());
		IAggregationChunkStorage aggregationChunkStorage = AggregationChunkStorage.create(reactor, stubChunkIdGenerator(), FRAME_FORMAT, fs);
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("date", ofLocalDate())
			.withDimension("advertiser", ofInt())
			.withDimension("campaign", ofInt())
			.withDimension("banner", ofInt())
			.withMeasure("impressions", sum(ofLong()))
			.withMeasure("clicks", sum(ofLong()))
			.withMeasure("conversions", sum(ofLong()))
			.withMeasure("revenue", sum(ofDouble()))
			.withAggregation(id("detailed")
				.withDimensions("date", "advertiser", "campaign", "banner")
				.withMeasures("impressions", "clicks", "conversions", "revenue"))
			.withAggregation(id("date")
				.withDimensions("date")
				.withMeasures("impressions", "clicks", "conversions", "revenue"))
			.withAggregation(id("advertiser")
				.withDimensions("advertiser")
				.withMeasures("impressions", "clicks", "conversions", "revenue"))
			.withRelation("campaign", "advertiser")
			.withRelation("banner", "campaign")
			.build();

		FileSystem fileSystem = FileSystem.create(reactor, EXECUTOR, logsDir);
		await(fileSystem.start());
		IMultilog<LogItem> multilog = Multilog.create(reactor,
			fileSystem,
			FrameFormats.lz4(),
			SerializerFactory.defaultInstance().create(CLASS_LOADER, LogItem.class),
			NAME_PARTITION_REMAINDER_SEQ);

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> logCubeStateManager = stateManagerFactory.create(cubeStructure, description);

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		LogProcessor<LogItem, ProtoCubeDiff, CubeDiff> logOTProcessor = LogProcessor.create(reactor, multilog,
			cubeExecutor.logStreamConsumer(LogItem.class), "testlog", List.of("partitionA"), logCubeStateManager);

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems1).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> finalLogCubeStateManager1 = logCubeStateManager;
		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager1, logOTProcessor);

		List<AggregationChunk> chunks = new ArrayList<>(logCubeStateManager.query(s -> s.getDataState().getAggregationState("date").getChunks().values()));
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Initialize cube with new structure (removed measure)
		cubeStructure = CubeStructure.builder()
			.withDimension("date", ofLocalDate())
			.withDimension("advertiser", ofInt())
			.withDimension("campaign", ofInt())
			.withDimension("banner", ofInt())
			.withMeasure("impressions", sum(ofLong()))
			.withMeasure("clicks", sum(ofLong()))
			.withMeasure("conversions", sum(ofLong()))
			.withMeasure("revenue", sum(ofDouble()))
			.withAggregation(id("detailed")
				.withDimensions("date", "advertiser", "campaign", "banner")
				.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
			.withAggregation(id("date")
				.withDimensions("date")
				.withMeasures("impressions", "clicks", "conversions")) // "revenue" measure is removed
			.withAggregation(id("advertiser")
				.withDimensions("advertiser")
				.withMeasures("impressions", "clicks", "conversions", "revenue"))
			.withRelation("campaign", "advertiser")
			.withRelation("banner", "campaign")
			.build();

		logCubeStateManager = stateManagerFactory.createUninitialized(cubeStructure, description);

		cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		stateManagerFactory.start(logCubeStateManager);

		logOTProcessor = LogProcessor.create(reactor, multilog, cubeExecutor.logStreamConsumer(LogItem.class),
			"testlog", List.of("partitionA"), logCubeStateManager);

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems2).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> finalLogCubeStateManager = logCubeStateManager;
		LogProcessor<LogItem, ProtoCubeDiff, CubeDiff> finalLogOTProcessor = logOTProcessor;
		await(finalLogCubeStateManager.catchUp());
		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager, finalLogOTProcessor);

		chunks = new ArrayList<>(logCubeStateManager.query(s -> s.getDataState().getAggregationState("date").getChunks().values()));
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertFalse(chunks.get(1).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(logCubeStateManager.query(s -> s.getDataState().getAggregationState("advertiser").getChunks().values()));
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertTrue(chunks.get(1).getMeasures().contains("revenue"));

		// Aggregate manually
		Map<Integer, Long> map = Stream.concat(listOfRandomLogItems1.stream(), listOfRandomLogItems2.stream())
			.collect(groupingBy(o -> o.date, reducing(0L, o -> o.clicks, Long::sum)));

		CubeReporting cubeReporting = CubeReporting.create(logCubeStateManager, cubeStructure, cubeExecutor);

		ToListStreamConsumer<LogItem> queryResultConsumer2 = ToListStreamConsumer.create();
		await(cubeReporting.queryRawStream(List.of("date"), List.of("clicks"), alwaysTrue(), LogItem.class, CLASS_LOADER).streamTo(
			queryResultConsumer2));

		// Check query results
		List<LogItem> queryResult2 = queryResultConsumer2.getList();
		for (LogItem logItem : queryResult2) {
			assertEquals(logItem.clicks, map.remove(logItem.date).longValue());
		}
		assertTrue(map.isEmpty());

		// Consolidate
		CubeConsolidator cubeConsolidator = CubeConsolidator.create(logCubeStateManager, cubeExecutor);
		CubeDiff consolidatingCubeDiff = await(cubeConsolidator.consolidate(List.copyOf(cubeStructure.getAggregationIds()), hotSegment()));
		assertFalse(consolidatingCubeDiff.isEmpty());

		chunks = new ArrayList<>(logCubeStateManager.query(s -> s.getDataState().getAggregationState("date").getChunks().values()));
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(logCubeStateManager.query(s -> s.getDataState().getAggregationState("advertiser").getChunks().values()));
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Query
		ToListStreamConsumer<LogItem> queryResultConsumer3 = ToListStreamConsumer.create();
		await(cubeReporting.queryRawStream(List.of("date"), List.of("clicks"), alwaysTrue(), LogItem.class, DefiningClassLoader.create(CLASS_LOADER))
			.streamTo(queryResultConsumer3));
		List<LogItem> queryResult3 = queryResultConsumer3.getList();

		// Check that query results before and after consolidation match
		assertEquals(queryResult2.size(), queryResult3.size());
		for (int i = 0; i < queryResult2.size(); ++i) {
			assertEquals(queryResult2.get(i).date, queryResult3.get(i).date);
			assertEquals(queryResult2.get(i).clicks, queryResult3.get(i).clicks);
		}
	}

	@Test
	public void testNewUnknownMeasureInAggregationDiffOnDeserialization() {
		{
			CubeStructure cubeStructure1 = CubeStructure.builder()
				.withDimension("date", ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withAggregation(id("date")
					.withDimensions("date")
					.withMeasures("impressions", "clicks", "conversions"))
				.build();

			StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> logCubeStateManager1 = stateManagerFactory.create(cubeStructure1, description);

			CubeExecutor cubeExecutor1 = CubeExecutor.create(reactor, cubeStructure1, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

			ILogDataConsumer<LogItem, ProtoCubeDiff> logStreamConsumer1 = cubeExecutor1.logStreamConsumer(LogItem.class);
			LogProcessor<LogItem, ProtoCubeDiff, CubeDiff> logOTProcessor1 = LogProcessor.create(reactor,
				multilog, logStreamConsumer1, "testlog", List.of("partitionA"), logCubeStateManager1);

			await(logCubeStateManager1.catchUp());

			await(StreamSuppliers.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
				StreamConsumers.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
		}

		// Initialize cube with new structure (remove "clicks" from cube configuration)
		CubeStructure cubeStructure2 = CubeStructure.builder()
			.withDimension("date", ofLocalDate())
			.withMeasure("impressions", sum(ofLong()))
			.withAggregation(id("date")
				.withDimensions("date")
				.withMeasures("impressions"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.createUninitialized(cubeStructure2, description);

		Throwable exception = assertThrows(Throwable.class, () -> stateManagerFactory.start(stateManager)).getCause();

		String expectedMessage;
		if (testName.equals("OT graph")) {
			exception = exception.getCause();
			assertThat(exception, instanceOf(JsonValidationException.class));
			expectedMessage = "Unknown fields: [clicks, conversions]";
		} else {
			expectedMessage = "Unknown measures [clicks, conversions] in aggregation 'date'";
			if (testName.equals("etcd state")) {
				exception = exception.getCause();
			}
		}
		assertEquals(expectedMessage, exception.getMessage());
	}

	@Test
	public void testUnknownAggregation() {
		{
			CubeStructure cubeStructure1 = CubeStructure.builder()
				.withDimension("date", ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withAggregation(id("date")
					.withDimensions("date")
					.withMeasures("impressions", "clicks"))
				.withAggregation(id("impressionsAggregation")
					.withDimensions("date")
					.withMeasures("impressions"))
				.withAggregation(id("otherAggregation")
					.withDimensions("date")
					.withMeasures("clicks"))
				.build();

			StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> logCubeStateManager1 = stateManagerFactory.create(cubeStructure1, description);

			CubeExecutor cubeExecutor1 = CubeExecutor.create(reactor, cubeStructure1, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

			ILogDataConsumer<LogItem, ProtoCubeDiff> logStreamConsumer1 = cubeExecutor1.logStreamConsumer(LogItem.class);

			LogProcessor<LogItem, ProtoCubeDiff, CubeDiff> logOTProcessor1 = LogProcessor.create(reactor,
				multilog, logStreamConsumer1, "testlog", List.of("partitionA"), logCubeStateManager1);

			await(logCubeStateManager1.catchUp());

			await(StreamSuppliers.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
				StreamConsumers.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
		}

		// Initialize cube with new structure (remove "impressions" aggregation from cube configuration)
		CubeStructure cubeStructure2 = CubeStructure.builder()
			.withDimension("date", ofLocalDate())
			.withMeasure("impressions", sum(ofLong()))
			.withMeasure("clicks", sum(ofLong()))
			.withAggregation(id("date")
				.withDimensions("date")
				.withMeasures("impressions", "clicks"))
			.withAggregation(id("otherAggregation")
				.withDimensions("date")
				.withMeasures("clicks"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.createUninitialized(cubeStructure2, description);

		Throwable t = assertThrows(Throwable.class, () -> stateManagerFactory.start(stateManager));
		assertThat(t.getMessage(), containsString("impressionsAggregation"));
	}
}
