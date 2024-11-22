package io.activej.cube.etcd;

import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.cube.aggregation.fieldtype.FieldTypes;
import io.activej.cube.aggregation.measure.Measures;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.etcd.CubeEtcdOTUplink.UplinkProtoCommit;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeOT;
import io.activej.etcd.exception.TransactionNotSucceededException;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogPositionDiff;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.OTState;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.AsyncOTUplink.FetchData;
import io.activej.reactor.Reactor;
import io.activej.test.rules.DescriptionRule;
import io.activej.test.rules.EventloopRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import java.util.*;

import static io.activej.common.collection.CollectionUtils.concat;
import static io.activej.cube.TestUtils.noFail;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CubeEtcdOTUplinkTest {
	public static final Random RANDOM = new Random(0);

	private static final List<String> MEASURES = List.of("a", "b", "c", "d");
	private static final PrimaryKey MIN_KEY = PrimaryKey.ofArray("100", "200");
	private static final PrimaryKey MAX_KEY = PrimaryKey.ofArray("300", "400");
	private static final int COUNT = 12345;
	private static final List<String> AGGREGATIONS = List.of("test", "aggr1", "aggr2");

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public DescriptionRule descriptionRule = new DescriptionRule();

	public static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	public static final Client ETCD_CLIENT = Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();

	private CubeEtcdOTUplink uplink;

	private long initialId;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Before
	public void setUp() throws Exception {
		JsonCodec<PrimaryKey> primaryKeyCodec = JsonCodecs.ofList((JsonCodec<Object>) (JsonCodec) JsonCodecs.ofString())
			.transform(PrimaryKey::values, PrimaryKey::ofList);
		Description description = descriptionRule.getDescription();
		ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());

		CubeStructure.Builder builder = CubeStructure.builder();

		for (String aggregation : AGGREGATIONS) {
			builder.withAggregation(CubeStructure.AggregationConfig.id(aggregation).withMeasures(MEASURES));
		}

		for (String measure : MEASURES) {
			builder.withMeasure(measure, Measures.sum(FieldTypes.ofInt()));
		}

		CubeStructure cubeStructure = builder.build();

		uplink = CubeEtcdOTUplink.builder(Reactor.getCurrentReactor(), cubeStructure, ETCD_CLIENT, root)
			.withChunkCodecsFactory($ -> new AggregationChunkJsonEtcdKVCodec(primaryKeyCodec))
			.build();

		noFail(uplink::delete);

		initialId = await(uplink.checkout()).commitId();
	}

	@Test
	public void checkoutEmpty() {
		FetchData<Long, LogDiff<CubeDiff>> result = await(uplink.checkout());

		assertEquals(initialId, (long) result.commitId());
		assertEquals(initialId, result.level());
		assertTrue(result.diffs().isEmpty());
	}

	@Test
	public void fetchEmpty() {
		FetchData<Long, LogDiff<CubeDiff>> result = await(uplink.fetch(initialId));

		assertEquals(initialId, result.level());
		assertEquals(initialId, (long) result.commitId());
		assertTrue(result.diffs().isEmpty());
	}

	@Test
	public void createProtoCommit() {
		List<LogDiff<CubeDiff>> diffs = initialDiffs();
		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(initialId, diffs, initialId));

		assertEquals(initialId, protoCommit.parentRevision());
		assertEquals(diffs, protoCommit.diffs());
	}

	@Test
	public void push() {
		List<LogDiff<CubeDiff>> diffs = initialDiffs();

		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(initialId, diffs, initialId));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		assertEquals(initialId + 1, (long) fetchData.commitId());
		assertEquals(initialId + 1, fetchData.level());
		assertDiffs(diffs, fetchData.diffs());
	}

	@Test
	public void removeChunks() {
		LogDiff<CubeDiff> diff = LogDiff.forCurrentPosition(CubeDiff.of(Map.of("aggr1", AggregationDiff.of(Set.of(chunk(100))))));
		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(initialId, List.of(diff), initialId));
		FetchData<Long, LogDiff<CubeDiff>> push1 = await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch(initialId));
		assertDiffs(List.of(diff), fetchData.diffs());

		diff = LogDiff.forCurrentPosition(CubeDiff.of(Map.of("aggr1", AggregationDiff.of(Set.of(), Set.of(chunk(100))))));
		protoCommit = await(uplink.createProtoCommit(push1.commitId(), List.of(diff), push1.level()));
		await(uplink.push(protoCommit));

		fetchData = await(uplink.fetch(initialId));
		assertDiffs(List.of(), fetchData.diffs());
	}

	@Test
	public void fetch() {
		List<LogDiff<CubeDiff>> totalDiffs = new ArrayList<>();

		for (int i = 0; i < 3; i++) {
			long index = initialId + i;
			List<LogDiff<CubeDiff>> diffs = nextDiffs(totalDiffs);
			UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(index, diffs, index));
			await(uplink.push(protoCommit));

			FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch(index));

			assertEquals(index + 1, (long) fetchData.commitId());
			assertEquals(index + 1, fetchData.level());
			assertDiffs(diffs, fetchData.diffs());

			totalDiffs.addAll(diffs);
		}

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch(initialId));

		assertEquals(initialId + 3, (long) fetchData.commitId());
		assertEquals(initialId + 3, fetchData.level());

		assertDiffs(totalDiffs, fetchData.diffs());
	}

	@Test
	public void chunkRemovalTwoCommits() {
		List<LogDiff<CubeDiff>> diffs = List.of(
			LogDiff.forCurrentPosition(CubeDiff.of(Map.of(
				"test", AggregationDiff.of(Set.of(chunk(10)))))));
		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(initialId, diffs, initialId));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		StubOTState state = new StubOTState();
		fetchData.diffs().forEach(state::apply);

		assertTrue(state.positions.isEmpty());
		assertEquals(Map.of(
			"test", Set.of(chunk(10))), state.chunks);

		diffs = List.of(
			LogDiff.forCurrentPosition(CubeDiff.of(Map.of(
				"test", AggregationDiff.of(Set.of(), Set.of(chunk(10)))))));
		protoCommit = await(uplink.createProtoCommit(initialId + 1, diffs, initialId + 1));
		await(uplink.push(protoCommit));

		fetchData = await(uplink.fetch(initialId + 1L));
		fetchData.diffs().forEach(state::apply);

		assertTrue(state.positions.isEmpty());
		assertTrue(state.chunks.isEmpty());

		FetchData<Long, LogDiff<CubeDiff>> checkoutData = await(uplink.checkout());

		assertEquals(initialId + 2, (long) checkoutData.commitId());
		assertEquals(initialId + 2, checkoutData.level());
		assertTrue(checkoutData.diffs().isEmpty());
	}

	@Test
	public void pushSameParent() {
		List<LogDiff<CubeDiff>> diffs1 = List.of(LogDiff.of(Map.of(
				"a", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("a1", 12), 100))),
			CubeDiff.of(Map.of(
				"aggr1", AggregationDiff.of(Set.of(chunk(1), chunk(2), chunk(3))),
				"aggr2", AggregationDiff.of(Set.of(chunk(4), chunk(5), chunk(6))))
			)
		));

		List<LogDiff<CubeDiff>> diffs2 = List.of(LogDiff.of(Map.of(
				"b", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("b1", 50), 200))),
			CubeDiff.of(Map.of(
				"aggr1", AggregationDiff.of(Set.of(chunk(10), chunk(20), chunk(30))),
				"aggr2", AggregationDiff.of(Set.of(chunk(40), chunk(50), chunk(60)))))
		));

		UplinkProtoCommit protoCommit1 = await(uplink.createProtoCommit(initialId, diffs1, initialId));
		UplinkProtoCommit protoCommit2 = await(uplink.createProtoCommit(initialId, diffs2, initialId));

		FetchData<Long, LogDiff<CubeDiff>> fetch1 = await(uplink.push(protoCommit1));
		assertEquals(initialId + 1, (long) fetch1.commitId());
		assertTrue(fetch1.diffs().isEmpty());

		FetchData<Long, LogDiff<CubeDiff>> fetch2 = await(uplink.push(protoCommit2));
		assertEquals(initialId + 2, (long) fetch2.commitId());
		assertDiffs(diffs1, fetch2.diffs());

		FetchData<Long, LogDiff<CubeDiff>> checkoutData = await(uplink.checkout());
		assertEquals(initialId + 2, (long) checkoutData.commitId());

		assertDiffs(OT_SYSTEM.squash(concat(diffs1, diffs2)), checkoutData.diffs());
	}

	@Test
	public void pushSameParentWithConflict() {
		List<LogDiff<CubeDiff>> initialDiffs = List.of(LogDiff.forCurrentPosition(
			CubeDiff.of(Map.of(
				"aggr1", AggregationDiff.of(Set.of(chunk(2), chunk(3), chunk(30), chunk(100)))))
		));

		await(uplink.push(await(uplink.createProtoCommit(initialId, initialDiffs, initialId))));

		List<LogDiff<CubeDiff>> diffs1 = List.of(LogDiff.of(Map.of(
				"a", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("a1", 12), 100))),
			CubeDiff.of(Map.of(
				"aggr1", AggregationDiff.of(
					Set.of(chunk(1)),
					Set.of(chunk(2), chunk(3)))))
		));

		List<LogDiff<CubeDiff>> diffs2 = List.of(LogDiff.of(Map.of(
				"b", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("b1", 50), 200))),
			CubeDiff.of(Map.of(
				"aggr1", AggregationDiff.of(
					Set.of(chunk(10)),
					Set.of(chunk(2), chunk(30))))
			)
		));

		UplinkProtoCommit protoCommit1 = await(uplink.createProtoCommit(initialId + 1L, diffs1, initialId + 1));
		UplinkProtoCommit protoCommit2 = await(uplink.createProtoCommit(initialId + 1L, diffs2, initialId + 1));

		FetchData<Long, LogDiff<CubeDiff>> fetch1 = await(uplink.push(protoCommit1));
		assertEquals(initialId + 2, (long) fetch1.commitId());
		assertTrue(fetch1.diffs().isEmpty());

		Exception exception = awaitException(uplink.push(protoCommit2));
		assertThat(exception, instanceOf(TransactionNotSucceededException.class));
	}

	@Test
	public void deleteAlreadyCleanedUpChunk() {
		List<LogDiff<CubeDiff>> diffs = List.of(LogDiff.forCurrentPosition(
			CubeDiff.of(Map.of(
				"aggregation", AggregationDiff.of(Set.of(), Set.of(chunk(100)))))
		));
		Throwable exception = awaitException(uplink.push(await(uplink.createProtoCommit(initialId, diffs, initialId))));

		assertThat(exception, instanceOf(TransactionNotSucceededException.class));
	}

	private static void assertDiffs(List<LogDiff<CubeDiff>> expected, List<LogDiff<CubeDiff>> actual) {
		StubOTState left = new StubOTState();
		StubOTState right = new StubOTState();

		expected.forEach(left::apply);
		actual.forEach(right::apply);

		assertEquals(left.chunks, right.chunks);
		assertEquals(left.positions, right.positions);
	}

	private static AggregationChunk chunk(long id) {
		return AggregationChunk.create(id, MEASURES, MIN_KEY, MAX_KEY, COUNT);
	}

	private static LogPosition randomLogPosition() {
		byte[] bytes = new byte[RANDOM.nextInt(10) + 1];
		RANDOM.nextBytes(bytes);
		return LogPosition.create(new LogFile(new String(bytes, UTF_8), RANDOM.nextInt()), RANDOM.nextLong());
	}

	private static List<LogDiff<CubeDiff>> initialDiffs() {
		return nextDiffs(List.of());
	}

	private static List<LogDiff<CubeDiff>> nextDiffs(List<LogDiff<CubeDiff>> prevDiffs) {
		List<LogDiff<CubeDiff>> diffs = new ArrayList<>();
		LogDiff<CubeDiff> prevReduced = LogDiff.reduce(prevDiffs, CubeDiff::reduce);

		int iLimit = RANDOM.nextInt(5);
		for (int i = 0; i < iLimit; i++) {
			Map<String, LogPositionDiff> partition = new HashMap<>();
			int jLimit = RANDOM.nextInt(5);
			for (int j = 0; j < jLimit; j++) {
				String aggregationId = i + "-" + j;
				LogPositionDiff prevPositionDiff = prevReduced.getPositions().get(aggregationId);
				LogPosition from = prevPositionDiff == null ? LogPosition.initial() : prevPositionDiff.to();
				partition.put(aggregationId, new LogPositionDiff(from, randomLogPosition()));
			}

			List<CubeDiff> cubeDiffs = new ArrayList<>();
			Map<String, AggregationDiff> prevCubeDiffs = prevReduced.getDiffs().size() == 1 ?
				prevReduced.getDiffs().get(0).getDiffs() :
				Map.of();

			jLimit = RANDOM.nextInt(5);
			for (int j = 0; j < jLimit; j++) {
				Map<String, AggregationDiff> aggregationDiffs = new HashMap<>();
				int kLimit = RANDOM.nextInt(5);
				for (int k = 0; k < kLimit; k++) {
					Set<AggregationChunk> added = new HashSet<>();
					int k1Limit = RANDOM.nextInt(5);
					for (int k1 = 0; k1 < k1Limit; k1++) {
						added.add(chunk(RANDOM.nextLong()));
					}
					String aggregationId = AGGREGATIONS.get(RANDOM.nextInt(AGGREGATIONS.size()));
					Set<AggregationChunk> removed = new HashSet<>();
					AggregationDiff aggregationDiff = prevCubeDiffs.get(aggregationId);
					if (aggregationDiff != null) {
						for (AggregationChunk addedChunk : aggregationDiff.getAddedChunks()) {
							if (RANDOM.nextBoolean()) {
								removed.add(addedChunk);
							}
						}
					}
					aggregationDiffs.put(aggregationId, AggregationDiff.of(added, removed));

				}
				cubeDiffs.add(CubeDiff.of(aggregationDiffs));
			}

			diffs.add(LogDiff.of(partition, cubeDiffs));
		}

		return diffs;
	}

	private static final class StubOTState implements OTState<LogDiff<CubeDiff>> {
		final Map<String, LogPosition> positions = new HashMap<>();
		final Map<String, Set<AggregationChunk>> chunks = new HashMap<>();

		@Override
		public void init() {
			positions.clear();
			chunks.clear();
		}

		@Override
		public void apply(LogDiff<CubeDiff> op) {
			for (Map.Entry<String, LogPositionDiff> entry : op.getPositions().entrySet()) {
				positions.put(entry.getKey(), entry.getValue().to());
			}
			for (CubeDiff diff : op.getDiffs()) {
				for (Map.Entry<String, AggregationDiff> entry : diff.entrySet()) {
					Set<AggregationChunk> set = chunks.computeIfAbsent(entry.getKey(), $ -> new HashSet<>());
					AggregationDiff aggregationDiff = entry.getValue();
					set.addAll(aggregationDiff.getAddedChunks());
					set.removeAll(aggregationDiff.getRemovedChunks());
					if (set.isEmpty()) {
						chunks.remove(entry.getKey());
					}
				}
			}
		}
	}
}
