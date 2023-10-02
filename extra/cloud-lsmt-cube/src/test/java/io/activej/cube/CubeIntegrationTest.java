package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.ChunkIdJsonCodec;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.LogOTState;
import io.activej.fs.FileSystem;
import io.activej.multilog.IMultilog;
import io.activej.multilog.Multilog;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.serializer.SerializerFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.cube.CubeConsolidator.ConsolidationStrategy.hotSegment;
import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.*;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.alwaysTrue;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CubeIntegrationTest extends CubeTestBase {
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void test() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		FileSystem fs = FileSystem.builder(reactor, EXECUTOR, aggregationsDir)
			.withTempDir(Files.createTempDirectory(""))
			.build();
		await(fs.start());
		FrameFormat frameFormat = FrameFormats.lz4();
		AggregationChunkStorage<Long> aggregationChunkStorage = AggregationChunkStorage.create(reactor, ChunkIdJsonCodec.ofLong(), AsyncSupplier.of(new RefLong(0)::inc), frameFormat, fs);
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("date", ofLocalDate())
			.withDimension("advertiser", ofInt())
			.withDimension("campaign", ofInt())
			.withDimension("banner", ofInt())
			.withRelation("campaign", "advertiser")
			.withRelation("banner", "campaign")
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
			.build();

		AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cubeStructure, description);

		CubeState cubeState = CubeState.create(cubeStructure);
		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cubeState);
		CubeExecutor cubeExecutor = CubeExecutor.builder(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage).build();
		CubeConsolidator cubeConsolidator = CubeConsolidator.create(cubeState, cubeStructure, cubeExecutor);

		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(reactor, LOG_OT, uplink, cubeDiffLogOTState);

		FileSystem fileSystem = FileSystem.create(reactor, EXECUTOR, logsDir);
		await(fileSystem.start());
		IMultilog<LogItem> multilog = Multilog.create(reactor,
			fileSystem,
			frameFormat,
			SerializerFactory.defaultInstance().create(CLASS_LOADER, LogItem.class),
			NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(reactor,
			multilog,
			cubeExecutor.logStreamConsumer(LogItem.class),
			"testlog",
			List.of("partitionA"),
			cubeDiffLogOTState);

		// checkout first (root) revision
		await(logCubeStateManager.checkout());

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));
		printDirContents(logsDir);

		//		AsynchronousFileChannel channel = AsynchronousFileChannel.open(Files.list(logsDir).findFirst().get(),
		//				EnumSet.of(StandardOpenOption.WRITE), EXECUTOR);
		//		channel.truncate(13);
		//		channel.write(ByteBuffer.wrap(new byte[]{123}), 0).get();
		//		channel.close();

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(300);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems2).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));
		printDirContents(logsDir);

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<LogItem> listOfRandomLogItems3 = LogItem.getListOfRandomLogItems(50);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems3).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));
		printDirContents(logsDir);

		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		await(aggregationChunkStorage.backup("backup1", (Set) cubeState.getAllChunks()));

		CubeReporting cubeReporting = CubeReporting.create(cubeState, cubeStructure, cubeExecutor);

		List<LogItem> logItems = await(cubeReporting.queryRawStream(List.of("date"), List.of("clicks"), alwaysTrue(),
				LogItem.class, DefiningClassLoader.create(CLASS_LOADER))
			.toList());

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);
		aggregateToMap(map, listOfRandomLogItems3);

		// Check query results
		assertEquals(map, logItems.stream().collect(toMap(r -> r.date, r -> r.clicks)));

		// Consolidate revision 4 as revision 5:
		CubeDiff consolidatingCubeDiff = await(cubeConsolidator.consolidate(hotSegment()));
		assertFalse(consolidatingCubeDiff.isEmpty());

		logCubeStateManager.add(LogDiff.forCurrentPosition(consolidatingCubeDiff));
		await(logCubeStateManager.sync());

		await(aggregationChunkStorage.finish(consolidatingCubeDiff.addedChunks().map(id -> (long) id).collect(toSet())));
		await(aggregationChunkStorage.cleanup((Set) cubeState.getAllChunks()));

		// Query
		List<LogItem> queryResult = await(cubeReporting.queryRawStream(List.of("date"), List.of("clicks"), alwaysTrue(),
			LogItem.class, DefiningClassLoader.create(CLASS_LOADER)).toList());

		assertEquals(map, queryResult.stream().collect(toMap(r -> r.date, r -> r.clicks)));

		// Check files in aggregations directory
		Set<String> actualChunkFileNames = Arrays.stream(checkNotNull(aggregationsDir.toFile().listFiles()))
			.map(File::getName)
			.collect(toSet());
		assertEquals(concat(Stream.of("backups"), cubeState.getAllChunks().stream().map(n -> n + ".log")).collect(toSet()),
			actualChunkFileNames);
	}

	private void printDirContents(Path logsDir) throws IOException {
		try (Stream<Path> list = Files.list(logsDir)) {
			list.forEach(System.out::println);
		}
	}

	private void aggregateToMap(Map<Integer, Long> map, List<LogItem> logItems) {
		for (LogItem logItem : logItems) {
			int date = logItem.date;
			long clicks = logItem.clicks;
			if (map.get(date) == null) {
				map.put(date, clicks);
			} else {
				Long clicksForDate = map.get(date);
				map.put(date, clicksForDate + clicks);
			}
		}
	}
}
