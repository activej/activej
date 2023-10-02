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
import io.activej.etl.LogProcessor;
import io.activej.fs.FileSystem;
import io.activej.multilog.IMultilog;
import io.activej.multilog.Multilog;
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

		TestStateManager stateManager = stateManagerFactory.create(cubeStructure, description);

		CubeExecutor cubeExecutor = CubeExecutor.builder(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage).build();
		CubeConsolidator cubeConsolidator = CubeConsolidator.create(stateManager.getCubeState(), cubeStructure, cubeExecutor);

		FileSystem fileSystem = FileSystem.create(reactor, EXECUTOR, logsDir);
		await(fileSystem.start());
		IMultilog<LogItem> multilog = Multilog.create(reactor,
			fileSystem,
			frameFormat,
			SerializerFactory.defaultInstance().create(CLASS_LOADER, LogItem.class),
			NAME_PARTITION_REMAINDER_SEQ);

		LogProcessor<LogItem, CubeDiff> logOTProcessor = LogProcessor.create(reactor,
			multilog,
			cubeExecutor.logStreamConsumer(LogItem.class),
			"testlog",
			List.of("partitionA"),
			stateManager.getLogState());

		// checkout first (root) revision
		stateManager.checkout();

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

		runProcessLogs(aggregationChunkStorage, stateManager, logOTProcessor);

		runProcessLogs(aggregationChunkStorage, stateManager, logOTProcessor);

		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(300);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems2).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));
		printDirContents(logsDir);

		runProcessLogs(aggregationChunkStorage, stateManager, logOTProcessor);

		List<LogItem> listOfRandomLogItems3 = LogItem.getListOfRandomLogItems(50);
		await(StreamSuppliers.ofIterable(listOfRandomLogItems3).streamTo(
			StreamConsumers.ofPromise(multilog.write("partitionA"))));
		printDirContents(logsDir);

		runProcessLogs(aggregationChunkStorage, stateManager, logOTProcessor);

		await(aggregationChunkStorage.backup("backup1", (Set) stateManager.getCubeState().query(CubeState::getAllChunks)));

		CubeReporting cubeReporting = CubeReporting.create(stateManager.getCubeState(), cubeStructure, cubeExecutor);

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

		stateManager.push(LogDiff.forCurrentPosition(consolidatingCubeDiff));

		await(aggregationChunkStorage.finish(consolidatingCubeDiff.addedChunks().map(id -> (long) id).collect(toSet())));
		await(aggregationChunkStorage.cleanup((Set) stateManager.getCubeState().query(CubeState::getAllChunks)));

		// Query
		List<LogItem> queryResult = await(cubeReporting.queryRawStream(List.of("date"), List.of("clicks"), alwaysTrue(),
			LogItem.class, DefiningClassLoader.create(CLASS_LOADER)).toList());

		assertEquals(map, queryResult.stream().collect(toMap(r -> r.date, r -> r.clicks)));

		// Check files in aggregations directory
		Set<String> actualChunkFileNames = Arrays.stream(checkNotNull(aggregationsDir.toFile().listFiles()))
			.map(File::getName)
			.collect(toSet());
		assertEquals(concat(Stream.of("backups"), stateManager.getCubeState().query(CubeState::getAllChunks).stream().map(n -> n + ".log")).collect(toSet()),
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
