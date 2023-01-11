package io.activej.cube;

import io.activej.aggregation.*;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.FrameFormat_LZ4;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.OTState_Log;
import io.activej.fs.Fs_Local;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.AsyncOTUplink;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Month;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.activej.aggregation.AggregationPredicates.gt;
import static io.activej.aggregation.PrimaryKey.ofArray;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube_Reactive.AggregationConfig.id;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public final class CubeGetIrrelevantChunksTest extends CubeTestBase {
	private static final int NUMBER_MIN = 0;
	private static final int NUMBER_MAX = 100;

	private static final int DATE_MIN_DAYS = (int) LocalDate.of(2020, Month.JANUARY, 1).toEpochDay();
	private static final int DATE_MAX_DAYS = (int) LocalDate.of(2021, Month.JANUARY, 1).toEpochDay();

	private static final LocalDate LOWER_DATE_BOUNDARY = LocalDate.of(2020, Month.JULY, 31);
	private static final int LOWER_DATE_BOUNDARY_DAYS = (int) LOWER_DATE_BOUNDARY.toEpochDay();
	private static final AggregationPredicate DATE_PREDICATE = gt("date", LOWER_DATE_BOUNDARY);
	private static final int LOWER_NUMBER_BOUNDARY = 50;
	private static final AggregationPredicate ADVERTISER_PREDICATE = gt("advertiser", LOWER_NUMBER_BOUNDARY);

	private OTStateManager<Long, LogDiff<CubeDiff>> stateManager;
	private AsyncAggregationChunkStorage<Long> chunkStorage;
	private Cube_Reactive.AggregationConfig dateAggregation;
	private Cube_Reactive.AggregationConfig advertiserDateAggregation;
	private AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink;
	private Cube_Reactive basicCube;
	private Cube_Reactive cube;

	private long chunkId;

	private final Set<Object> toBePreserved = new HashSet<>();
	private final Set<Object> toBeCleanedUp = new HashSet<>();

	@Before
	public void before() throws Exception {
		chunkId = 0;
		toBeCleanedUp.clear();
		toBePreserved.clear();
		Path aggregationsDir = temporaryFolder.newFolder().toPath();

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

		basicCube = createBasicCube()
				.withAggregation(dateAggregation)
				.withAggregation(advertiserDateAggregation);

		OTState_Log<CubeDiff> cubeDiffLogOTState = OTState_Log.create(basicCube);
		uplink = uplinkFactory.create(basicCube);
		stateManager = OTStateManager.create(reactor, LOG_OT, uplink, cubeDiffLogOTState);
		await(stateManager.checkout());
	}

	@Test
	public void date() {
		cube = createBasicCube().withAggregation(dateAggregation.withPredicate(DATE_PREDICATE));

		toBePreserved.add(addChunk("date", ofArray(DATE_MIN_DAYS), ofArray(DATE_MAX_DAYS)));
		toBePreserved.add(addChunk("date", ofArray(DATE_MIN_DAYS + 50), ofArray(DATE_MAX_DAYS - 50)));

		toBeCleanedUp.add(addChunk("date", ofArray(DATE_MIN_DAYS), ofArray(LOWER_DATE_BOUNDARY_DAYS)));
		toBeCleanedUp.add(addChunk("date", ofArray(DATE_MIN_DAYS), ofArray(DATE_MIN_DAYS + 50)));

		doTest();
	}

	@Test
	public void advertiserDate_DatePredicate() {
		cube = createBasicCube().withAggregation(advertiserDateAggregation.withPredicate(DATE_PREDICATE));

		toBePreserved.add(addChunk("advertiser-date", ofArray(5, DATE_MIN_DAYS), ofArray(6, DATE_MAX_DAYS)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(1, DATE_MIN_DAYS + 50), ofArray(20, DATE_MAX_DAYS - 50)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(6, DATE_MIN_DAYS), ofArray(7, DATE_MIN_DAYS + 1)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(10, DATE_MIN_DAYS), ofArray(10, LOWER_DATE_BOUNDARY_DAYS + 1)));

		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(5, DATE_MIN_DAYS), ofArray(5, DATE_MIN_DAYS + 50)));
		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(10, DATE_MIN_DAYS + 50), ofArray(10, LOWER_DATE_BOUNDARY_DAYS)));

		doTest();
	}

	@Test
	public void advertiserDate_AdvertiserPredicate() {
		cube = createBasicCube().withAggregation(advertiserDateAggregation.withPredicate(ADVERTISER_PREDICATE));

		toBePreserved.add(addChunk("advertiser-date", ofArray(NUMBER_MIN, DATE_MIN_DAYS), ofArray(LOWER_NUMBER_BOUNDARY + 1, DATE_MAX_DAYS)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(NUMBER_MIN + 50, DATE_MIN_DAYS + 50), ofArray(NUMBER_MAX - 10, DATE_MAX_DAYS - 50)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(LOWER_NUMBER_BOUNDARY - 1, DATE_MIN_DAYS), ofArray(LOWER_NUMBER_BOUNDARY + 1, DATE_MIN_DAYS + 1)));

		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(NUMBER_MIN, DATE_MIN_DAYS), ofArray(LOWER_NUMBER_BOUNDARY, DATE_MIN_DAYS + 50)));
		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(LOWER_NUMBER_BOUNDARY - 10, DATE_MIN_DAYS + 50), ofArray(LOWER_NUMBER_BOUNDARY - 5, LOWER_DATE_BOUNDARY_DAYS)));

		doTest();
	}

	@Test
	public void advertiserDate_AdvertiserPredicateAndDatePredicate() {
		cube = createBasicCube().withAggregation(advertiserDateAggregation
				.withPredicate(AggregationPredicates.and(ADVERTISER_PREDICATE, DATE_PREDICATE)));

		toBePreserved.add(addChunk("advertiser-date", ofArray(NUMBER_MIN, DATE_MIN_DAYS), ofArray(LOWER_NUMBER_BOUNDARY + 1, DATE_MAX_DAYS)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(NUMBER_MIN + 50, DATE_MIN_DAYS + 50), ofArray(NUMBER_MAX - 10, DATE_MAX_DAYS - 50)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(LOWER_NUMBER_BOUNDARY - 1, DATE_MIN_DAYS), ofArray(LOWER_NUMBER_BOUNDARY + 1, DATE_MIN_DAYS + 1)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(NUMBER_MAX - 10, DATE_MAX_DAYS - 50), ofArray(NUMBER_MAX - 5, DATE_MAX_DAYS - 25)));
		toBePreserved.add(addChunk("advertiser-date", ofArray(NUMBER_MAX - 10, DATE_MAX_DAYS - 50), ofArray(NUMBER_MAX - 10, DATE_MAX_DAYS - 25)));

		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(NUMBER_MIN, DATE_MIN_DAYS), ofArray(LOWER_NUMBER_BOUNDARY, DATE_MAX_DAYS - 50)));
		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(LOWER_NUMBER_BOUNDARY - 10, DATE_MIN_DAYS + 50), ofArray(LOWER_NUMBER_BOUNDARY - 5, LOWER_DATE_BOUNDARY_DAYS + 10)));
		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(LOWER_NUMBER_BOUNDARY - 10, DATE_MIN_DAYS + 50), ofArray(LOWER_NUMBER_BOUNDARY - 10, LOWER_DATE_BOUNDARY_DAYS + 10)));
		toBeCleanedUp.add(addChunk("advertiser-date", ofArray(NUMBER_MIN + 10, DATE_MAX_DAYS - 50), ofArray(NUMBER_MIN + 10, DATE_MAX_DAYS - 25)));

		doTest();
	}

	private void doTest() {
		await(stateManager.sync());

		Set<Object> expectedChunks = new HashSet<>();
		expectedChunks.addAll(toBePreserved);
		expectedChunks.addAll(toBeCleanedUp);

		assertEquals(expectedChunks, basicCube.getAllChunks());

		stateManager = OTStateManager.create(reactor, LOG_OT, uplink, OTState_Log.create(cube));
		await(stateManager.checkout());

		Set<Object> irrelevantChunks = cube.getIrrelevantChunks()
				.values()
				.stream()
				.flatMap(Collection::stream)
				.map(AggregationChunk::getChunkId)
				.collect(toSet());
		assertEquals(toBeCleanedUp, irrelevantChunks);
	}

	private long addChunk(String aggregationId, PrimaryKey minKey, PrimaryKey maxKey) {
		long chunkId = ++this.chunkId;
		stateManager.add(LogDiff.forCurrentPosition(
				CubeDiff.of(Map.of(
						aggregationId, AggregationDiff.of(Set.of(
								AggregationChunk.create(
										chunkId,
										cube.getAggregation(aggregationId).getMeasures(),
										minKey,
										maxKey,
										1))))
				)));
		return chunkId;
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
}
