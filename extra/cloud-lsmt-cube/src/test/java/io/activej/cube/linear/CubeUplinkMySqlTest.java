package io.activej.cube.linear;

import com.dslplatform.json.StringConverter;
import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.util.JsonCodec;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.exception.StateFarAheadException;
import io.activej.cube.linear.CubeUplinkMySql.UplinkProtoCommit;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeOT;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogPositionDiff;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.OTState;
import io.activej.ot.exception.OTException;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplink.FetchData;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.common.Utils.*;
import static io.activej.cube.TestUtils.initializeUplink;
import static io.activej.cube.linear.CubeUplinkMySqlTest.SimplePositionDiff.diff;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.dataSource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CubeUplinkMySqlTest {
	public static final Random RANDOM = ThreadLocalRandom.current();

	private static final List<String> MEASURES = Arrays.asList("a", "b", "c", "d");
	private static final PrimaryKey MIN_KEY = PrimaryKey.ofArray("100", "200");
	private static final PrimaryKey MAX_KEY = PrimaryKey.ofArray("300", "400");
	private static final int COUNT = 12345;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	public static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	private DataSource dataSource;
	private CubeUplinkMySql uplink;

	@SuppressWarnings({"unchecked", "rawtypes", "ConstantConditions"})
	@Before
	public void setUp() throws Exception {
		dataSource = dataSource("test.properties");

		JsonCodec<PrimaryKey> primaryKeyCodec = JsonCodec.of(
				jsonReader -> PrimaryKey.ofArray(jsonReader.readArray(StringConverter.READER, new String[0])),
				(jsonWriter, primaryKey) -> jsonWriter.serialize((List) Arrays.asList(primaryKey.getArray()), StringConverter.WRITER)
		);

		PrimaryKeyCodecs codecs = PrimaryKeyCodecs.ofLookUp($ -> primaryKeyCodec);
		uplink = CubeUplinkMySql.create(Executors.newCachedThreadPool(), dataSource, codecs);

		initializeUplink(uplink);
	}

	@Test
	public void checkoutEmpty() {
		FetchData<Long, LogDiff<CubeDiff>> result = await(uplink.checkout());

		assertEquals(0, result.getLevel());
		assertEquals(0, (long) result.getCommitId());
		assertTrue(result.getDiffs().isEmpty());
	}

	@Test
	public void fetchEmpty() {
		FetchData<Long, LogDiff<CubeDiff>> result = await(uplink.fetch(0L));

		assertEquals(0, result.getLevel());
		assertEquals(0, (long) result.getCommitId());
		assertTrue(result.getDiffs().isEmpty());
	}

	@Test
	public void createProtoCommit() {
		List<LogDiff<CubeDiff>> diffs = randomDiffs();
		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));

		assertEquals(0, protoCommit.getParentRevision());
		assertEquals(diffs, protoCommit.getDiffs());
	}

	@Test
	public void push() {
		List<LogDiff<CubeDiff>> diffs = randomDiffs();

		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		assertEquals(1L, (long) fetchData.getCommitId());
		assertEquals(1L, fetchData.getLevel());
		assertDiffs(diffs, fetchData.getDiffs());
	}

	@Test
	public void fetch() {
		List<LogDiff<CubeDiff>> totalDiffs = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			List<LogDiff<CubeDiff>> diffs = randomDiffs();
			UplinkProtoCommit protoCommit = await(uplink.createProtoCommit((long) i, diffs, i));
			await(uplink.push(protoCommit));

			FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch((long) i));

			assertEquals(i + 1, (long) fetchData.getCommitId());
			assertEquals(i + 1, fetchData.getLevel());
			assertDiffs(diffs, fetchData.getDiffs());

			totalDiffs.addAll(diffs);
		}

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.fetch(0L));

		assertEquals(3, (long) fetchData.getCommitId());
		assertEquals(3, fetchData.getLevel());
		assertDiffs(totalDiffs, fetchData.getDiffs());
	}

	@Test
	public void chunkRemovalSameCommit() {
		List<LogDiff<CubeDiff>> diffs = Arrays.asList(
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(setOf(chunk(10)))))),
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(emptySet(), setOf(chunk(10))))))
		);
		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		assertTrue(fetchData.getDiffs().isEmpty());
	}

	@Test
	public void chunkRemovalTwoCommits() {
		List<LogDiff<CubeDiff>> diffs = singletonList(
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(setOf(chunk(10)))))));
		UplinkProtoCommit protoCommit = await(uplink.createProtoCommit(0L, diffs, 0));
		await(uplink.push(protoCommit));

		FetchData<Long, LogDiff<CubeDiff>> fetchData = await(uplink.checkout());

		StubState state = new StubState();
		fetchData.getDiffs().forEach(state::apply);

		assertTrue(state.positions.isEmpty());
		assertEquals(mapOf("test", setOf(chunk(10))), state.chunks);

		diffs = singletonList(
				LogDiff.forCurrentPosition(CubeDiff.of(mapOf(
						"test", AggregationDiff.of(emptySet(), setOf(chunk(10)))))));
		protoCommit = await(uplink.createProtoCommit(1L, diffs, 1));
		await(uplink.push(protoCommit));

		fetchData = await(uplink.fetch(1L));
		fetchData.getDiffs().forEach(state::apply);

		assertTrue(state.positions.isEmpty());
		assertTrue(state.chunks.isEmpty());

		FetchData<Long, LogDiff<CubeDiff>> checkoutData = await(uplink.checkout());

		assertEquals(2, (long) checkoutData.getCommitId());
		assertEquals(2, checkoutData.getLevel());
		assertTrue(checkoutData.getDiffs().isEmpty());
	}

	@Test
	public void pushSameParent() {
		List<LogDiff<CubeDiff>> diffs1 = singletonList(LogDiff.of(
				mapOf(
						"a", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("a1", 12), 100))
				),
				CubeDiff.of(
						mapOf(
								"aggr1", AggregationDiff.of(setOf(chunk(1), chunk(2), chunk(3))),
								"aggr2", AggregationDiff.of(setOf(chunk(4), chunk(5), chunk(6)))
						)
				)
		));

		List<LogDiff<CubeDiff>> diffs2 = singletonList(LogDiff.of(
				mapOf(
						"b", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("b1", 50), 200))
				),
				CubeDiff.of(
						mapOf(
								"aggr1", AggregationDiff.of(setOf(chunk(10), chunk(20), chunk(30))),
								"aggr2", AggregationDiff.of(setOf(chunk(40), chunk(50), chunk(60)))
						)
				)
		));

		UplinkProtoCommit protoCommit1 = await(uplink.createProtoCommit(0L, diffs1, 0));
		UplinkProtoCommit protoCommit2 = await(uplink.createProtoCommit(0L, diffs2, 0));

		FetchData<Long, LogDiff<CubeDiff>> fetch1 = await(uplink.push(protoCommit1));
		assertEquals(1, (long) fetch1.getCommitId());
		assertTrue(fetch1.getDiffs().isEmpty());

		FetchData<Long, LogDiff<CubeDiff>> fetch2 = await(uplink.push(protoCommit2));
		assertEquals(2, (long) fetch2.getCommitId());
		assertEquals(diffs1, fetch2.getDiffs());

		FetchData<Long, LogDiff<CubeDiff>> checkoutData = await(uplink.checkout());
		assertEquals(2, (long) checkoutData.getCommitId());

		assertEquals(OT_SYSTEM.squash(concat(diffs1, diffs2)), checkoutData.getDiffs());
	}

	@Test
	public void pushSameParentWithConflict() {
		List<LogDiff<CubeDiff>> initialDiffs = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(
						mapOf(
								"aggr1", AggregationDiff.of(setOf(chunk(2), chunk(3), chunk(30), chunk(100)))
						)
				)
		));

		await(uplink.push(await(uplink.createProtoCommit(0L, initialDiffs, 0))));

		List<LogDiff<CubeDiff>> diffs1 = singletonList(LogDiff.of(
				mapOf(
						"a", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("a1", 12), 100))
				),
				CubeDiff.of(
						mapOf(
								"aggr1", AggregationDiff.of(
										setOf(chunk(1)),
										setOf(chunk(2), chunk(3))
								)
						)
				)
		));

		List<LogDiff<CubeDiff>> diffs2 = singletonList(LogDiff.of(
				mapOf(
						"b", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("b1", 50), 200))
				),
				CubeDiff.of(
						mapOf(
								"aggr1", AggregationDiff.of(
										setOf(chunk(10)),
										setOf(chunk(2), chunk(30)))
						)
				)
		));

		UplinkProtoCommit protoCommit1 = await(uplink.createProtoCommit(1L, diffs1, 1));
		UplinkProtoCommit protoCommit2 = await(uplink.createProtoCommit(1L, diffs2, 1));

		FetchData<Long, LogDiff<CubeDiff>> fetch1 = await(uplink.push(protoCommit1));
		assertEquals(2, (long) fetch1.getCommitId());
		assertTrue(fetch1.getDiffs().isEmpty());

		Exception exception = awaitException(uplink.push(protoCommit2));
		assertThat(exception, instanceOf(SQLIntegrityConstraintViolationException.class));
		assertEquals("Chunk 2 is already removed in revision 2", exception.getMessage());
	}

	@Test
	/*
			+----------+-----------+-------------+
			| Chunk ID | Added rev | Removed rev |
			+----------+-----------+-------------+
			|        1 |         1 | <null>      |
			|        2 |         1 | 2           |
			|        3 |         1 | 3           |
			|        4 |         1 | 5           |
			|        5 |         3 | <null>      |
			|        6 |         3 | 4           |
			|        7 |         3 | 5           |
			|        8 |         5 | <null>      |
			|        9 |         5 | 6           |
			+----------+-----------+-------------+
	*/
	public void fetchChunkDiffs() throws SQLException, MalformedDataException {
		List<LogDiff<CubeDiff>> diffsRev1 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggr1", AggregationDiff.of(
						setOf(chunk(1), chunk(2), chunk(3), chunk(4))
				)))));
		await(uplink.push(await(uplink.createProtoCommit(0L, diffsRev1, 0))));

		List<LogDiff<CubeDiff>> diffsRev2 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggr1", AggregationDiff.of(
						emptySet(),
						setOf(chunk(2))
				)))));
		await(uplink.push(await(uplink.createProtoCommit(1L, diffsRev2, 1))));

		List<LogDiff<CubeDiff>> diffsRev3 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggr1", AggregationDiff.of(
						setOf(chunk(5), chunk(6), chunk(7)),
						setOf(chunk(3))
				)))));
		await(uplink.push(await(uplink.createProtoCommit(2L, diffsRev3, 2))));

		List<LogDiff<CubeDiff>> diffsRev4 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggr1", AggregationDiff.of(
						emptySet(),
						setOf(chunk(6))
				)))));
		await(uplink.push(await(uplink.createProtoCommit(3L, diffsRev4, 3))));

		List<LogDiff<CubeDiff>> diffsRev5 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggr1", AggregationDiff.of(
						setOf(chunk(8), chunk(9)),
						setOf(chunk(4), chunk(7))
				)))));
		await(uplink.push(await(uplink.createProtoCommit(4L, diffsRev5, 4))));

		List<LogDiff<CubeDiff>> diffsRev6 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggr1", AggregationDiff.of(
						emptySet(),
						setOf(chunk(9))
				)))));
		await(uplink.push(await(uplink.createProtoCommit(5L, diffsRev6, 5))));

		try (Connection connection = dataSource.getConnection()) {
			{
				CubeDiff diff0to1 = uplink.fetchChunkDiffs(connection, 0, 1);
				assertCubeDiff(setOf(1L, 2L, 3L, 4L), emptySet(), diff0to1);

				CubeDiff diff0to2 = uplink.fetchChunkDiffs(connection, 0, 2);
				assertCubeDiff(setOf(1L, 3L, 4L), emptySet(), diff0to2);

				CubeDiff diff0to3 = uplink.fetchChunkDiffs(connection, 0, 3);
				assertCubeDiff(setOf(1L, 4L, 5L, 6L, 7L), emptySet(), diff0to3);

				CubeDiff diff0to4 = uplink.fetchChunkDiffs(connection, 0, 4);
				assertCubeDiff(setOf(1L, 4L, 5L, 7L), emptySet(), diff0to4);

				CubeDiff diff0to5 = uplink.fetchChunkDiffs(connection, 0, 5);
				assertCubeDiff(setOf(1L, 5L, 8L, 9L), emptySet(), diff0to5);

				CubeDiff diff0to6 = uplink.fetchChunkDiffs(connection, 0, 6);
				assertCubeDiff(setOf(1L, 5L, 8L), emptySet(), diff0to6);
			}

			{
				CubeDiff diff1to2 = uplink.fetchChunkDiffs(connection, 1, 2);
				assertCubeDiff(emptySet(), setOf(2L), diff1to2);

				CubeDiff diff1to3 = uplink.fetchChunkDiffs(connection, 1, 3);
				assertCubeDiff(setOf(5L, 6L, 7L), setOf(2L, 3L), diff1to3);

				CubeDiff diff1to4 = uplink.fetchChunkDiffs(connection, 1, 4);
				assertCubeDiff(setOf(5L, 7L), setOf(2L, 3L), diff1to4);

				CubeDiff diff1to5 = uplink.fetchChunkDiffs(connection, 1, 5);
				assertCubeDiff(setOf(5L, 8L, 9L), setOf(2L, 3L, 4L), diff1to5);

				CubeDiff diff1to6 = uplink.fetchChunkDiffs(connection, 1, 6);
				assertCubeDiff(setOf(5L, 8L), setOf(2L, 3L, 4L), diff1to6);
			}

			{
				CubeDiff diff2to3 = uplink.fetchChunkDiffs(connection, 2, 3);
				assertCubeDiff(setOf(5L, 6L, 7L), setOf(3L), diff2to3);

				CubeDiff diff2to4 = uplink.fetchChunkDiffs(connection, 2, 4);
				assertCubeDiff(setOf(5L, 7L), setOf(3L), diff2to4);

				CubeDiff diff2to5 = uplink.fetchChunkDiffs(connection, 2, 5);
				assertCubeDiff(setOf(5L, 8L, 9L), setOf(3L, 4L), diff2to5);

				CubeDiff diff2to6 = uplink.fetchChunkDiffs(connection, 2, 6);
				assertCubeDiff(setOf(5L, 8L), setOf(3L, 4L), diff2to6);
			}

			{
				CubeDiff diff3to4 = uplink.fetchChunkDiffs(connection, 3, 4);
				assertCubeDiff(emptySet(), setOf(6L), diff3to4);

				CubeDiff diff3to5 = uplink.fetchChunkDiffs(connection, 3, 5);
				assertCubeDiff(setOf(8L, 9L), setOf(4L, 6L, 7L), diff3to5);

				CubeDiff diff3to6 = uplink.fetchChunkDiffs(connection, 3, 6);
				assertCubeDiff(setOf(8L), setOf(4L, 6L, 7L), diff3to6);
			}

			{
				CubeDiff diff4to5 = uplink.fetchChunkDiffs(connection, 4, 5);
				assertCubeDiff(setOf(8L, 9L), setOf(4L, 7L), diff4to5);

				CubeDiff diff4to6 = uplink.fetchChunkDiffs(connection, 4, 6);
				assertCubeDiff(setOf(8L), setOf(4L, 7L), diff4to6);
			}

			{
				CubeDiff diff5to6 = uplink.fetchChunkDiffs(connection, 5, 6);
				assertCubeDiff(emptySet(), setOf(9L), diff5to6);
			}
		}
	}

	@Test
	/*
			+----------+-----------+----------+
			| Revision | Partition | Position |
			+----------+-----------+----------+
			|        1 | a         |      101 |
			|        1 | b         |      102 |
			|        1 | c         |      103 |
			|        2 | a         |      201 |
			|        2 | b         |      202 |
			|        3 | a         |      301 |
			|        4 | a         |      401 |
			+----------+-----------+----------+
	*/
	public void fetchPositionDiffs() throws SQLException {
		List<LogDiff<CubeDiff>> diffsRev1 = singletonList(LogDiff.of(
				mapOf(
						"a", new LogPositionDiff(
								LogPosition.initial(),
								LogPosition.create(new LogFile("a", 0), 101)),
						"b", new LogPositionDiff(
								LogPosition.initial(),
								LogPosition.create(new LogFile("b", 0), 102)),
						"c", new LogPositionDiff(
								LogPosition.initial(),
								LogPosition.create(new LogFile("c", 0), 103))
				), emptyList()
		));
		await(uplink.push(await(uplink.createProtoCommit(0L, diffsRev1, 0))));

		List<LogDiff<CubeDiff>> diffsRev2 = singletonList(LogDiff.of(
				mapOf(
						"a", new LogPositionDiff(
								LogPosition.create(new LogFile("a", 0), 101),
								LogPosition.create(new LogFile("a", 0), 201)),
						"b", new LogPositionDiff(
								LogPosition.create(new LogFile("b", 0), 102),
								LogPosition.create(new LogFile("b", 0), 202))
				), emptyList()
		));
		await(uplink.push(await(uplink.createProtoCommit(1L, diffsRev2, 1))));

		List<LogDiff<CubeDiff>> diffsRev3 = singletonList(LogDiff.of(
				mapOf(
						"a", new LogPositionDiff(
								LogPosition.create(new LogFile("a", 0), 201),
								LogPosition.create(new LogFile("a", 0), 301))
				), emptyList()
		));
		await(uplink.push(await(uplink.createProtoCommit(2L, diffsRev3, 2))));

		List<LogDiff<CubeDiff>> diffsRev4 = singletonList(LogDiff.of(
				mapOf(
						"a", new LogPositionDiff(
								LogPosition.create(new LogFile("a", 0), 301),
								LogPosition.create(new LogFile("a", 0), 401))
				), emptyList()
		));
		await(uplink.push(await(uplink.createProtoCommit(3L, diffsRev4, 3))));

		try (Connection connection = dataSource.getConnection()) {
			{
				Map<String, LogPositionDiff> diff0to1 = uplink.fetchPositionDiffs(connection, 0, 1);
				assertPositionDiff(mapOf(
						"a", diff(0, 101),
						"b", diff(0, 102),
						"c", diff(0, 103)
				), diff0to1);

				Map<String, LogPositionDiff> diff0to2 = uplink.fetchPositionDiffs(connection, 0, 2);
				assertPositionDiff(mapOf(
						"a", diff(0, 201),
						"b", diff(0, 202),
						"c", diff(0, 103)
				), diff0to2);

				Map<String, LogPositionDiff> diff0to3 = uplink.fetchPositionDiffs(connection, 0, 3);
				assertPositionDiff(mapOf(
						"a", diff(0, 301),
						"b", diff(0, 202),
						"c", diff(0, 103)
				), diff0to3);

				Map<String, LogPositionDiff> diff0to4 = uplink.fetchPositionDiffs(connection, 0, 4);
				assertPositionDiff(mapOf(
						"a", diff(0, 401),
						"b", diff(0, 202),
						"c", diff(0, 103)
				), diff0to4);
			}

			{
				Map<String, LogPositionDiff> diff1to2 = uplink.fetchPositionDiffs(connection, 1, 2);
				assertPositionDiff(mapOf(
						"a", diff(101, 201),
						"b", diff(102, 202)
				), diff1to2);

				Map<String, LogPositionDiff> diff1to3 = uplink.fetchPositionDiffs(connection, 1, 3);
				assertPositionDiff(mapOf(
						"a", diff(101, 301),
						"b", diff(102, 202)
				), diff1to3);

				Map<String, LogPositionDiff> diff1to4 = uplink.fetchPositionDiffs(connection, 1, 4);
				assertPositionDiff(mapOf(
						"a", diff(101, 401),
						"b", diff(102, 202)
				), diff1to4);
			}

			{
				Map<String, LogPositionDiff> diff2to3 = uplink.fetchPositionDiffs(connection, 2, 3);
				assertPositionDiff(mapOf(
						"a", diff(201, 301)
				), diff2to3);

				Map<String, LogPositionDiff> diff2to4 = uplink.fetchPositionDiffs(connection, 2, 4);
				assertPositionDiff(mapOf(
						"a", diff(201, 401)
				), diff2to4);
			}

			{
				Map<String, LogPositionDiff> diff3to4 = uplink.fetchPositionDiffs(connection, 3, 4);
				assertPositionDiff(mapOf(
						"a", diff(301, 401)
				), diff3to4);
			}
		}
	}

	@Test
	public void deleteAlreadyCleanedUpChunk() {
		List<LogDiff<CubeDiff>> diffs = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggregation", AggregationDiff.of(emptySet(), setOf(chunk(100)))))
		));
		Throwable exception = awaitException(uplink.push(await(uplink.createProtoCommit(0L, diffs, 0))));

		assertThat(exception, instanceOf(OTException.class));
		assertEquals("Chunk is already removed", exception.getMessage());
	}

	@Test
	public void fetchStateFarAhead() throws SQLException {
		List<LogDiff<CubeDiff>> diffs1 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggregation", AggregationDiff.of(setOf(chunk(1), chunk(2)))))
		));
		await(uplink.push(await(uplink.createProtoCommit(0L, diffs1, 0))));

		List<LogDiff<CubeDiff>> diffs2 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggregation", AggregationDiff.of(setOf(chunk(3), chunk(4)))))
		));
		await(uplink.push(await(uplink.createProtoCommit(1L, diffs2, 1))));

		List<LogDiff<CubeDiff>> diffs3 = singletonList(LogDiff.forCurrentPosition(
				CubeDiff.of(mapOf("aggregation", AggregationDiff.of(setOf(chunk(5), chunk(6)))))
		));
		await(uplink.push(await(uplink.createProtoCommit(2L, diffs3, 2))));

		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("DELETE FROM " + CubeUplinkMySql.REVISION_TABLE +
						" WHERE `revision` IN (0,1,2)");
			}
		}

		Throwable exception = awaitException(uplink.fetch(1L));
		assertThat(exception, instanceOf(StateFarAheadException.class));
		StateFarAheadException e = (StateFarAheadException) exception;
		assertEquals(1L, e.getStartingRevision());
		assertEquals(setOf(1L, 2L), e.getMissingRevisions());
	}

	private static void assertCubeDiff(Set<Long> added, Set<Long> removed, CubeDiff actual) {
		Set<Long> actualAdded = new HashSet<>();
		Set<Long> actualRemoved = new HashSet<>();
		for (Map.Entry<String, AggregationDiff> entry : actual.entrySet()) {
			for (AggregationChunk chunk : entry.getValue().getAddedChunks()) {
				actualAdded.add((Long) chunk.getChunkId());
			}
			for (AggregationChunk chunk : entry.getValue().getRemovedChunks()) {
				actualRemoved.add((Long) chunk.getChunkId());
			}
		}
		assertTrue(intersection(actualAdded, actualRemoved).isEmpty());
		assertEquals(added, actualAdded);
		assertEquals(removed, actualRemoved);
	}

	private static void assertPositionDiff(Map<String, SimplePositionDiff> expected, Map<String, LogPositionDiff> actual) {
		assertEquals(expected.size(), actual.size());
		for (Map.Entry<String, LogPositionDiff> entry : actual.entrySet()) {
			String partition = entry.getKey();
			SimplePositionDiff simplePositionDiff = expected.get(partition);
			LogPositionDiff actualPositionDiff = entry.getValue();
			LogPosition actualFrom = actualPositionDiff.from;
			LogPosition actualTo = actualPositionDiff.to;

			assertEquals(simplePositionDiff.to, actualTo.getPosition());

			if (simplePositionDiff.from == 0L) {
				assertEquals(LogPosition.initial(), actualFrom);
			} else {
				assertEquals(partition, actualFrom.getLogFile().getName());
				assertEquals(simplePositionDiff.from, actualFrom.getPosition());
			}
			assertEquals(partition, actualTo.getLogFile().getName());
		}
	}

	static class SimplePositionDiff {
		private final long from;
		private final long to;

		private SimplePositionDiff(long from, long to) {
			this.from = from;
			this.to = to;
		}

		static SimplePositionDiff diff(long from, long to) {
			return new SimplePositionDiff(from, to);
		}
	}

	private static void assertDiffs(List<LogDiff<CubeDiff>> expected, List<LogDiff<CubeDiff>> actual) {
		StubState left = new StubState();
		StubState right = new StubState();

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

	private static List<LogDiff<CubeDiff>> randomDiffs() {
		List<LogDiff<CubeDiff>> diffs = new ArrayList<>();

		for (int i = 0; i < RANDOM.nextInt(5); i++) {
			Map<String, LogPositionDiff> positions = new HashMap<>();
			for (int j = 0; j < RANDOM.nextInt(5); j++) {
				LogPosition from;
				if (RANDOM.nextFloat() < 0.1) {
					from = LogPosition.initial();
				} else {
					from = randomLogPosition();
				}
				positions.put(i + "-" + j, new LogPositionDiff(from, randomLogPosition()));
			}

			List<CubeDiff> cubeDiffs = new ArrayList<>();
			for (int j = 0; j < RANDOM.nextInt(5); j++) {
				Map<String, AggregationDiff> aggregationDiffs = new HashMap<>();
				for (int k = 0; k < RANDOM.nextInt(5); k++) {
					Set<AggregationChunk> added = new HashSet<>();
					for (int k1 = 0; k1 < RANDOM.nextInt(5); k1++) {
						added.add(chunk(RANDOM.nextLong()));
					}
					aggregationDiffs.put(i + "-" + j + "-" + k, AggregationDiff.of(added));

				}
				cubeDiffs.add(CubeDiff.of(aggregationDiffs));
			}

			diffs.add(LogDiff.of(positions, cubeDiffs));
		}

		return diffs;
	}

	private static final class StubState implements OTState<LogDiff<CubeDiff>> {
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
				positions.put(entry.getKey(), entry.getValue().to);
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
