package io.activej.cube;

import io.activej.aggregation.AggregationChunkStorage_Reactive;
import io.activej.aggregation.AggregationPredicate;
import io.activej.aggregation.AsyncAggregationChunkStorage;
import io.activej.aggregation.JsonCodec_ChunkId;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.FrameFormat_LZ4;
import io.activej.cube.Cube_Reactive.AggregationConfig;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.service.CubeConsolidationController;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.OTState_Log;
import io.activej.fs.Fs_Local;
import io.activej.multilog.AsyncMultilog;
import io.activej.multilog.Multilog_Reactive;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.serializer.SerializerBuilder;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.aggregation.AggregationPredicates.alwaysTrue;
import static io.activej.aggregation.AggregationPredicates.gt;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.common.Utils.keysToMap;
import static io.activej.cube.Cube_Reactive.AggregationConfig.id;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CubeRemovingOfIrrelevantChunksTest extends CubeTestBase {
	private static final int numberMin = 0;
	private static final int numberMax = 100;

	private static final int dateMin = (int) LocalDate.of(2020, Month.JANUARY, 1).toEpochDay();
	private static final int dateMax = (int) LocalDate.of(2021, Month.JANUARY, 1).toEpochDay();
	private static final LocalDate LOWER_DATE_BOUNDARY = LocalDate.of(2020, Month.JULY, 31);
	private static final int LOWER_DATE_BOUNDARY_DAYS = (int) LOWER_DATE_BOUNDARY.toEpochDay();
	private static final AggregationPredicate DATE_PREDICATE = gt("date", LOWER_DATE_BOUNDARY);

	private AsyncAggregationChunkStorage<Long> chunkStorage;
	private AggregationConfig dateAggregation;
	private AggregationConfig advertiserDateAggregation;
	private AggregationConfig campaignBannerDateAggregation;
	private AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink;

	@Before
	public void before() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Fs_Local fs = Fs_Local.create(reactor, EXECUTOR, aggregationsDir)
				.withTempDir(Files.createTempDirectory(""));
		await(fs.start());
		FrameFormat frameFormat = FrameFormat_LZ4.create();
		chunkStorage = AggregationChunkStorage_Reactive.create(reactor, JsonCodec_ChunkId.ofLong(), AsyncSupplier.of(new RefLong(0)::inc), frameFormat, fs);

		dateAggregation = id("date")
				.withDimensions("date")
				.withMeasures("impressions", "clicks", "conversions", "revenue");

		advertiserDateAggregation = id("advertiser-date")
				.withDimensions("advertiser", "date")
				.withMeasures("impressions", "clicks", "conversions", "revenue");

		campaignBannerDateAggregation = id("campaign-banner-date")
				.withDimensions("campaign", "banner", "date")
				.withMeasures("impressions", "clicks", "conversions", "revenue");

		Cube_Reactive basicCube = createBasicCube()
				.withAggregation(dateAggregation)
				.withAggregation(advertiserDateAggregation)
				.withAggregation(campaignBannerDateAggregation);

		OTState_Log<CubeDiff> cubeDiffLogOTState = OTState_Log.create(basicCube);
		uplink = uplinkFactory.create(basicCube);

		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(reactor, LOG_OT, uplink, cubeDiffLogOTState);

		Fs_Local localFs = Fs_Local.create(reactor, EXECUTOR, logsDir);
		await(localFs.start());
		AsyncMultilog<LogItem> multilog = Multilog_Reactive.create(reactor,
				localFs,
				frameFormat,
				SerializerBuilder.create(CLASS_LOADER).build(LogItem.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(reactor,
				multilog,
				basicCube.logStreamConsumer(LogItem.class),
				"testlog",
				List.of("partitionA"),
				cubeDiffLogOTState);

		// checkout first (root) revision
		await(stateManager.checkout());

		// Save and aggregate logs
		List<LogItem> allLogItems = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
			if (i == 0) {
				rangeLogItemsOutOfPredicate(listOfRandomLogItems);
			} else {
				randomRangeLogItems(listOfRandomLogItems);
			}

			await(StreamSupplier.ofIterable(listOfRandomLogItems).streamTo(
					StreamConsumer.ofPromise(multilog.write("partitionA"))));

			runProcessLogs(chunkStorage, stateManager, logOTProcessor);
			allLogItems.addAll(listOfRandomLogItems);
		}

		List<LogItem> logItems = await(basicCube.queryRawStream(List.of("date"), List.of("clicks"), alwaysTrue(),
						LogItem.class, CLASS_LOADER)
				.toList());

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, allLogItems);

		assertEquals(map, logItems.stream().collect(toMap(r -> r.date, r -> r.clicks)));
	}

	@Test
	public void test() {
		Cube_Reactive cube = createBasicCube()
				.withAggregation(dateAggregation.withPredicate(DATE_PREDICATE))
				.withAggregation(advertiserDateAggregation.withPredicate(DATE_PREDICATE))
				.withAggregation(campaignBannerDateAggregation.withPredicate(DATE_PREDICATE));
		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(reactor, LOG_OT, uplink, OTState_Log.create(cube));
		await(stateManager.checkout());

		CubeConsolidationController<Long, LogDiff<CubeDiff>, Long> consolidationController =
				CubeConsolidationController.create(reactor, DIFF_SCHEME, cube, stateManager, chunkStorage);

		Map<String, Integer> chunksBefore = getChunksByAggregation(cube);
		await(consolidationController.cleanupIrrelevantChunks());

		Map<String, Integer> chunksAfter = getChunksByAggregation(cube);

		for (Map.Entry<String, Integer> afterEntry : chunksAfter.entrySet()) {
			String key = afterEntry.getKey();
			Integer before = chunksBefore.get(key);
			Integer after = afterEntry.getValue();
			assertTrue(after < before);
			System.out.println("Removed " + (before - after) + " chunks form aggregation'" + key + '\'');
		}
	}

	private static Map<String, Integer> getChunksByAggregation(Cube_Reactive cube) {
		return keysToMap(cube.getAggregationIds().stream(), id -> cube.getAggregation(id).getChunks());
	}

	private Cube_Reactive createBasicCube() {
		return Cube_Reactive.create(reactor, EXECUTOR, CLASS_LOADER, chunkStorage)
				.withDimension("date", ofLocalDate())
				.withDimension("advertiser", ofInt())
				.withDimension("campaign", ofInt())
				.withDimension("banner", ofInt())
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()));
	}

	private void randomRangeLogItems(List<LogItem> logItems) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		boolean breakPredicate = random.nextBoolean();
		int sameAdvertiser = random.nextBoolean() ? random.nextInt(numberMin, numberMax + 1) : -1;
		int sameCampaign = random.nextBoolean() ? random.nextInt(numberMin, numberMax + 1) : -1;
		int sameBanner = random.nextBoolean() ? random.nextInt(numberMin, numberMax + 1) : -1;
		for (LogItem logItem : logItems) {
			logItem.advertiser = sameAdvertiser == -1 ? random.nextInt(numberMin, numberMax + 1) : sameAdvertiser;
			logItem.campaign = sameCampaign == -1 ? random.nextInt(numberMin, numberMax + 1) : sameCampaign;
			logItem.banner = sameBanner == -1 ? random.nextInt(numberMin, numberMax + 1) : sameBanner;
			logItem.date = (int) random.nextLong(dateMin, breakPredicate ? LOWER_DATE_BOUNDARY_DAYS + 1 : dateMax + 1);
		}
	}

	private void rangeLogItemsOutOfPredicate(List<LogItem> logItems) {
		for (LogItem logItem : logItems) {
			logItem.advertiser = 0;
			logItem.campaign = 0;
			logItem.banner = 0;
			logItem.date = dateMin;
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
