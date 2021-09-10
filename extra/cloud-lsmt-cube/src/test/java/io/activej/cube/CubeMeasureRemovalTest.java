package io.activej.cube;

import com.dslplatform.json.ParsingException;
import io.activej.aggregation.*;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.LogDataConsumer;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.LogOTState;
import io.activej.fs.LocalActiveFs;
import io.activej.multilog.Multilog;
import io.activej.multilog.MultilogImpl;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.OTUplink;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.activej.aggregation.AggregationPredicates.alwaysTrue;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubeMeasureRemovalTest extends CubeTestBase {
	private static final FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	private AggregationChunkStorage<Long> aggregationChunkStorage;
	private Multilog<LogItem> multilog;
	private Path aggregationsDir;
	private Path logsDir;

	@Before
	public void before() throws IOException {
		aggregationsDir = temporaryFolder.newFolder().toPath();
		logsDir = temporaryFolder.newFolder().toPath();

		LocalActiveFs fs = LocalActiveFs.create(EVENTLOOP, EXECUTOR, aggregationsDir);
		await(fs.start());
		aggregationChunkStorage = ActiveFsChunkStorage.create(EVENTLOOP, ChunkIdCodec.ofLong(), new IdGeneratorStub(), FRAME_FORMAT, fs);
		BinarySerializer<LogItem> serializer = SerializerBuilder.create(CLASS_LOADER).build(LogItem.class);
		LocalActiveFs localFs = LocalActiveFs.create(EVENTLOOP, EXECUTOR, logsDir);
		await(localFs.start());
		multilog = MultilogImpl.create(EVENTLOOP,
				localFs,
				LZ4FrameFormat.create(),
				serializer,
				NAME_PARTITION_REMAINDER_SEQ);
	}

	@Test
	public void test() {
		LocalActiveFs fs = LocalActiveFs.create(EVENTLOOP, EXECUTOR, aggregationsDir);
		await(fs.start());
		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(EVENTLOOP, ChunkIdCodec.ofLong(), new IdGeneratorStub(), FRAME_FORMAT, fs);
		Cube cube = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
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
				.withRelation("banner", "campaign");

		OTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cube);

		LocalActiveFs localFs = LocalActiveFs.create(EVENTLOOP, EXECUTOR, logsDir);
		await(localFs.start());
		Multilog<LogItem> multilog = MultilogImpl.create(EVENTLOOP,
				localFs,
				LZ4FrameFormat.create(),
				SerializerBuilder.create(CLASS_LOADER).build(LogItem.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(EVENTLOOP, LOG_OT, uplink, cubeDiffLogOTState);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(EVENTLOOP, multilog,
				cube.logStreamConsumer(LogItem.class), "testlog", asList("partitionA"), cubeDiffLogOTState);

		// checkout first (root) revision
		await(logCubeStateManager.checkout());

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems1 = LogItem.getListOfRandomLogItems(100);
		await(StreamSupplier.ofIterable(listOfRandomLogItems1).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));

		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager1 = logCubeStateManager;
		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager1, logOTProcessor);

		List<AggregationChunk> chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Initialize cube with new structure (removed measure)
		cube = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
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
				.withRelation("banner", "campaign");

		LogOTState<CubeDiff> cubeDiffLogOTState1 = LogOTState.create(cube);
		logCubeStateManager = OTStateManager.create(EVENTLOOP, LOG_OT, uplink, cubeDiffLogOTState1);

		logOTProcessor = LogOTProcessor.create(EVENTLOOP, multilog, cube.logStreamConsumer(LogItem.class),
				"testlog", asList("partitionA"), cubeDiffLogOTState1);

		await(logCubeStateManager.checkout());

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(100);
		await(StreamSupplier.ofIterable(listOfRandomLogItems2).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));

		OTStateManager<Long, LogDiff<CubeDiff>> finalLogCubeStateManager = logCubeStateManager;
		LogOTProcessor<LogItem, CubeDiff> finalLogOTProcessor = logOTProcessor;
		await(finalLogCubeStateManager.sync());
		runProcessLogs(aggregationChunkStorage, finalLogCubeStateManager, finalLogOTProcessor);

		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertFalse(chunks.get(1).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(2, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));
		assertTrue(chunks.get(1).getMeasures().contains("revenue"));

		// Aggregate manually
		Map<Integer, Long> map = Stream.concat(listOfRandomLogItems1.stream(), listOfRandomLogItems2.stream())
				.collect(groupingBy(o -> o.date, reducing(0L, o -> o.clicks, Long::sum)));

		StreamConsumerToList<LogItem> queryResultConsumer2 = StreamConsumerToList.create();
		await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, CLASS_LOADER).streamTo(
				queryResultConsumer2));

		// Check query results
		List<LogItem> queryResult2 = queryResultConsumer2.getList();
		for (LogItem logItem : queryResult2) {
			assertEquals(logItem.clicks, map.remove(logItem.date).longValue());
		}
		assertTrue(map.isEmpty());

		// Consolidate
		CubeDiff consolidatingCubeDiff = await(cube.consolidate(Aggregation::consolidateHotSegment));
		await(aggregationChunkStorage.finish(consolidatingCubeDiff.addedChunks().map(id -> (long) id).collect(toSet())));
		assertFalse(consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		await(logCubeStateManager.sync());

		chunks = new ArrayList<>(cube.getAggregation("date").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertFalse(chunks.get(0).getMeasures().contains("revenue"));

		chunks = new ArrayList<>(cube.getAggregation("advertiser").getState().getChunks().values());
		assertEquals(1, chunks.size());
		assertTrue(chunks.get(0).getMeasures().contains("revenue"));

		// Query
		StreamConsumerToList<LogItem> queryResultConsumer3 = StreamConsumerToList.create();
		await(cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class, DefiningClassLoader.create(CLASS_LOADER))
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
			Cube cube1 = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
					.withDimension("date", ofLocalDate())
					.withMeasure("impressions", sum(ofLong()))
					.withMeasure("clicks", sum(ofLong()))
					.withMeasure("conversions", sum(ofLong()))
					.withAggregation(id("date")
							.withDimensions("date")
							.withMeasures("impressions", "clicks", "conversions"));

			OTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cube1);

			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(EVENTLOOP, LOG_OT, uplink, cubeDiffLogOTState);

			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);
			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(EVENTLOOP,
					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			await(logCubeStateManager1.checkout());

			await(StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
					StreamConsumer.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
		}

		// Initialize cube with new structure (remove "clicks" from cube configuration)
		Cube cube2 = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions"));

		OTUplink<Long, LogDiff<CubeDiff>, ?> uplink2 = uplinkFactory.createUninitialized(cube2);

		Throwable exception = awaitException(uplink2.checkout());
		assertThat(exception, instanceOf(MalformedDataException.class));

		String expectedMessage;
		if (testName.equals("OT graph")) {
			exception = exception.getCause();
			assertThat(exception, instanceOf(ParsingException.class));
			expectedMessage = "Unknown fields: [clicks, conversions]";
		} else {
			expectedMessage = "Unknown measures [clicks, conversions] in aggregation 'date'";
		}
		assertEquals(expectedMessage, exception.getMessage());
	}

	@Test
	public void testUnknownAggregation() {
		{
			Cube cube1 = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
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
							.withMeasures("clicks"));

			OTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cube1);

			LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube1);
			OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager1 = OTStateManager.create(EVENTLOOP, LOG_OT, uplink, cubeDiffLogOTState);

			LogDataConsumer<LogItem, CubeDiff> logStreamConsumer1 = cube1.logStreamConsumer(LogItem.class);

			LogOTProcessor<LogItem, CubeDiff> logOTProcessor1 = LogOTProcessor.create(EVENTLOOP,
					multilog, logStreamConsumer1, "testlog", asList("partitionA"), cubeDiffLogOTState);

			await(logCubeStateManager1.checkout());

			await(StreamSupplier.ofIterable(LogItem.getListOfRandomLogItems(100)).streamTo(
					StreamConsumer.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(aggregationChunkStorage, logCubeStateManager1, logOTProcessor1);
		}

		// Initialize cube with new structure (remove "impressions" aggregation from cube configuration)
		Cube cube2 = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks"))
				.withAggregation(id("otherAggregation")
						.withDimensions("date")
						.withMeasures("clicks"));

		OTUplink<Long, LogDiff<CubeDiff>, ?> uplink2 = uplinkFactory.createUninitialized(cube2);

		Throwable exception = awaitException(uplink2.checkout());
		assertThat(exception, instanceOf(MalformedDataException.class));
		if (testName.equals("OT graph")) {
			exception = exception.getCause();
			assertThat(exception, instanceOf(ParsingException.class));
		}
		assertEquals("Unknown aggregation: impressionsAggregation", exception.getMessage());
	}
}
