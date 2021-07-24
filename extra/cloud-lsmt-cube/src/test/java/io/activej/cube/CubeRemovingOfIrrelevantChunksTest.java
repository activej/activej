package io.activej.cube;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.AggregationPredicate;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.Cube.AggregationConfig;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffCodec;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.cube.ot.CubeOT;
import io.activej.cube.service.CubeConsolidationController;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.*;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.multilog.Multilog;
import io.activej.multilog.MultilogImpl;
import io.activej.ot.OTCommit;
import io.activej.ot.OTStateManager;
import io.activej.ot.repository.OTRepositoryMySql;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplinkImpl;
import io.activej.serializer.SerializerBuilder;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.aggregation.AggregationPredicates.alwaysTrue;
import static io.activej.aggregation.AggregationPredicates.gt;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.common.collection.CollectionUtils.keysToMap;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.cube.TestUtils.initializeRepository;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CubeRemovingOfIrrelevantChunksTest {
	private static final CubeDiffScheme<LogDiff<CubeDiff>> DIFF_SCHEME = CubeDiffScheme.ofLogDiffs();
	private static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();
	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static final int numberMin = 0;
	private static final int numberMax = 100;

	private static final int dateMin = (int) LocalDate.of(2020, Month.JANUARY, 1).toEpochDay();
	private static final int dateMax = (int) LocalDate.of(2021, Month.JANUARY, 1).toEpochDay();
	private static final LocalDate LOWER_DATE_BOUNDARY = LocalDate.of(2020, Month.JULY, 31);
	private static final int LOWER_DATE_BOUNDARY_DAYS = (int) LOWER_DATE_BOUNDARY.toEpochDay();
	private static final AggregationPredicate DATE_PREDICATE = gt("date", LOWER_DATE_BOUNDARY);

	private Eventloop eventloop;
	private ActiveFsChunkStorage<Long> chunkStorage;
	private Executor executor;
	private AggregationConfig dateAggregation;
	private AggregationConfig advertiserDateAggregation;
	private AggregationConfig campaignBannerDateAggregation;
	private OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node;

	@Before
	public void before() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.getCurrentEventloop();
		executor = Executors.newCachedThreadPool();

		LocalActiveFs fs = LocalActiveFs.create(eventloop, executor, aggregationsDir)
				.withTempDir(Files.createTempDirectory(""));
		await(fs.start());
		FrameFormat frameFormat = LZ4FrameFormat.create();
		chunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), frameFormat, fs);

		dateAggregation = id("date")
				.withDimensions("date")
				.withMeasures("impressions", "clicks", "conversions", "revenue");

		advertiserDateAggregation = id("advertiser-date")
				.withDimensions("advertiser", "date")
				.withMeasures("impressions", "clicks", "conversions", "revenue");

		campaignBannerDateAggregation = id("campaign-banner-date")
				.withDimensions("campaign", "banner", "date")
				.withMeasures("impressions", "clicks", "conversions", "revenue");

		Cube basicCube = createBasicCube()
				.withAggregation(dateAggregation)
				.withAggregation(advertiserDateAggregation)
				.withAggregation(campaignBannerDateAggregation);

		DataSource dataSource = dataSource("test.properties");
		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
				OT_SYSTEM, LogDiffCodec.create(CubeDiffCodec.create(basicCube)));
		initializeRepository(repository);

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(basicCube);
		node = OTUplinkImpl.create(repository, OT_SYSTEM);
		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(eventloop, OT_SYSTEM, node, cubeDiffLogOTState);

		LocalActiveFs localFs = LocalActiveFs.create(eventloop, executor, logsDir);
		await(localFs.start());
		Multilog<LogItem> multilog = MultilogImpl.create(eventloop,
				localFs,
				frameFormat,
				SerializerBuilder.create(CLASS_LOADER).build(LogItem.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<LogItem, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				multilog,
				basicCube.logStreamConsumer(LogItem.class),
				"testlog",
				singletonList("partitionA"),
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

		List<LogItem> logItems = await(basicCube.queryRawStream(singletonList("date"), singletonList("clicks"), alwaysTrue(),
				LogItem.class, CLASS_LOADER)
				.toList());

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, allLogItems);

		assertEquals(map, logItems.stream().collect(toMap(r -> r.date, r -> r.clicks)));
	}

	@Test
	public void test() {
		Cube cube = createBasicCube()
				.withAggregation(dateAggregation.withPredicate(DATE_PREDICATE))
				.withAggregation(advertiserDateAggregation.withPredicate(DATE_PREDICATE))
				.withAggregation(campaignBannerDateAggregation.withPredicate(DATE_PREDICATE));
		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(eventloop, OT_SYSTEM, node, LogOTState.create(cube));
		await(stateManager.checkout());

		CubeConsolidationController<Long, LogDiff<CubeDiff>, Long> consolidationController =
				CubeConsolidationController.create(eventloop, DIFF_SCHEME, cube, stateManager, chunkStorage);

		Map<String, Integer> chunksBefore = getChunksByAggregation(cube);
		await(consolidationController.cleanupIrrelevantChunks());

		Map<String, Integer> chunksAfter = getChunksByAggregation(cube);

		for (Map.Entry<String, Integer> afterEntry : chunksAfter.entrySet()) {
			String key = afterEntry.getKey();
			Integer before = chunksBefore.get(key);
			Integer after = afterEntry.getValue();
			assertTrue(after < before);
			System.out.println("Removed " + (before - after) + " chunks form aggregation'" + key +'\'');
		}
	}

	private static Map<String, Integer> getChunksByAggregation(Cube cube) {
		return keysToMap(cube.getAggregationIds().stream(), id -> cube.getAggregation(id).getChunks());
	}

	private Cube createBasicCube() {
		return Cube.create(eventloop, executor, CLASS_LOADER, chunkStorage)
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
