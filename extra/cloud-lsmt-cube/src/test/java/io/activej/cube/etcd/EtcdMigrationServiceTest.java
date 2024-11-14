package io.activej.cube.etcd;

import io.activej.cube.AggregationState;
import io.activej.cube.CubeState;
import io.activej.cube.CubeStructure;
import io.activej.cube.CubeTestBase;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.etl.LogState;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.StateManager;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.options.DeleteOption;
import org.jetbrains.annotations.Nullable;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.promise.TestUtils.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class EtcdMigrationServiceTest extends CubeTestBase {
	public static final String PUB_AGGREGATION_ID = "pub";
	public static final String ADV_AGGREGATION_ID = "adv";

	private final Random random = ThreadLocalRandom.current();

	private CubeStructure cubeStructure;
	private ByteSequence migrationRoot;

	@Before
	@Override
	public void setUp() throws Exception {
		Assume.assumeTrue("No need to test etcd -> etcd migration", !testName.contains("etcd"));

		super.setUp();

		migrationRoot = byteSequenceFrom("test.migration." + description.getClassName() + "#" + description.getMethodName());

		ETCD_CLIENT.getKVClient().delete(migrationRoot, DeleteOption.builder().isPrefix(true).build()).get();

		cubeStructure = CubeStructure.builder()
			.withDimension("pub", ofInt())
			.withDimension("adv", ofInt())
			.withMeasure("pubRequests", sum(ofLong()))
			.withMeasure("advRequests", sum(ofLong()))
			.withAggregation(id(PUB_AGGREGATION_ID)
				.withDimensions("pub")
				.withMeasures("pubRequests"))
			.withAggregation(id(ADV_AGGREGATION_ID)
				.withDimensions("adv")
				.withMeasures("advRequests"))
			.build();
	}

	@Test
	public void testMigration() throws Exception {
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);

		for (int i = 0; i < 10; i++) {
			LogDiff<CubeDiff> diff = generateLogDiff(stateManager);
			await(stateManager.push(List.of(diff)));
		}

		EtcdMigrationService migrationService = EtcdMigrationService.builder(stateManager, ETCD_CLIENT.getKVClient(), migrationRoot)
			.withChunkCodecsFactoryJson(cubeStructure)
			.build();

		TxnResponse txnResponse = migrationService.migrate().get();
		assertTrue(txnResponse.isSucceeded());

		CubeEtcdStateManager etcdStateManager = createEtcdStateManager(cubeStructure);
		etcdStateManager.start();

		LogState<CubeDiff, CubeState> expected = stateManager.query(state -> state);
		LogState<CubeDiff, CubeState> actual = etcdStateManager.query(state -> state);

		assertStates(expected, actual);
	}

	@Test
	public void testMigrationToNotEmptyRoot() throws InterruptedException, ExecutionException {
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		EtcdMigrationService migrationService = EtcdMigrationService.builder(stateManager, ETCD_CLIENT.getKVClient(), migrationRoot)
			.withChunkCodecsFactoryJson(cubeStructure)
			.build();

		ETCD_CLIENT.getKVClient().put(migrationRoot.concat(byteSequenceFrom(".inner")), ByteSequence.EMPTY).get();

		ExecutionException e = assertThrows(ExecutionException.class, () -> migrationService.migrate().get());
		assertThat(e.getMessage(), containsString("namespace is not empty"));
	}

	private long currentChunkId;

	private LogDiff<CubeDiff> generateLogDiff(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateQueryFn) {
		Map<String, LogPositionDiff> positionDiffs = new HashMap<>();
		Map<String, AggregationDiff> aggregationOps = new HashMap<>();

		for (int i = 0; i < 10; i++) {
			if (random.nextBoolean()) continue;

			String partition = "partition-" + i;
			LogPosition existing = stateQueryFn.query(state -> state.getPositions().get(partition));
			if (existing == null) {
				LogPosition logPosition = LogPosition.create(new LogFile("file-" + i, 0), 100);
				positionDiffs.put(partition, new LogPositionDiff(LogPosition.initial(), logPosition));
			} else {
				LogPosition logPosition = LogPosition.create(existing.getLogFile(), existing.getPosition() + 100);
				positionDiffs.put(partition, new LogPositionDiff(existing, logPosition));
			}

			String aggregation = random.nextBoolean() ? ADV_AGGREGATION_ID : PUB_AGGREGATION_ID;

			Set<AggregationChunk> added = new HashSet<>();
			Set<AggregationChunk> deleted = new HashSet<>();

			for (int j = 0; j < 10; j++) {
				if (random.nextBoolean()) {
					long chunkId = currentChunkId++;
					int minKey = random.nextInt();
					int maxKey = minKey + random.nextInt(1000) + 1;
					added.add(AggregationChunk.create(chunkId, List.of(),
						PrimaryKey.ofArray(minKey),
						PrimaryKey.ofArray(maxKey),
						random.nextInt()
					));
				} else {
					Collection<AggregationChunk> chunks = stateQueryFn.query(state -> state.getDataState()
						.getAggregationState(aggregation)
						.getChunks()
						.values());
					AggregationChunk randomChunk = getRandomChunk(chunks);
					if (randomChunk != null) {
						deleted.add(randomChunk);
					}
				}
			}

			aggregationOps.put(aggregation, AggregationDiff.of(added, deleted));

		}
		return LogDiff.of(positionDiffs, CubeDiff.of(aggregationOps));
	}

	private @Nullable AggregationChunk getRandomChunk(Collection<AggregationChunk> chunks) {
		if (chunks.isEmpty()) return null;
		int c = random.nextInt(chunks.size());
		Iterator<AggregationChunk> iterator = chunks.iterator();
		AggregationChunk result = null;
		for (int i = 0; i < c; i++) {
			result = iterator.next();
		}
		return result;
	}

	private CubeEtcdStateManager createEtcdStateManager(CubeStructure cubeStructure) {
		return CubeEtcdStateManager.builder(ETCD_CLIENT, migrationRoot, cubeStructure)
			.build();
	}

	private void assertStates(LogState<CubeDiff, CubeState> expected, LogState<CubeDiff, CubeState> actual) {
		assertEquals(expected.getPositions(), actual.getPositions());

		assertDataStates(expected.getDataState(), actual.getDataState());
	}

	private void assertDataStates(CubeState expected, CubeState actual) {
		Map<String, AggregationState> expectedStates = expected.getAggregationStates();
		Map<String, AggregationState> actualStates = actual.getAggregationStates();

		assertEquals(expectedStates.keySet(), actualStates.keySet());

		for (Map.Entry<String, AggregationState> entry : expectedStates.entrySet()) {
			AggregationState expectedState = entry.getValue();
			AggregationState actualState = actualStates.get(entry.getKey());

			Map<Long, AggregationChunk> expectedChunks = expectedState.getChunks();
			Map<Long, AggregationChunk> actualChunks = actualState.getChunks();

			assertEquals(expectedChunks.keySet(), actualChunks.keySet());

			for (Map.Entry<Long, AggregationChunk> chunkEntry : expectedChunks.entrySet()) {
				AggregationChunk expectedChunk = chunkEntry.getValue();
				AggregationChunk actualChunk = actualChunks.get(chunkEntry.getKey());

				assertChunks(expectedChunk, actualChunk);
			}
		}
	}

	private void assertChunks(AggregationChunk expectedChunk, AggregationChunk actualChunk) {
		assertEquals(expectedChunk.getChunkId(), actualChunk.getChunkId());
		assertEquals(expectedChunk.getMeasures(), actualChunk.getMeasures());
		assertEquals(expectedChunk.getCount(), actualChunk.getCount());
		assertEquals(expectedChunk.getMinPrimaryKey(), actualChunk.getMinPrimaryKey());
		assertEquals(expectedChunk.getMaxPrimaryKey(), actualChunk.getMaxPrimaryKey());
	}
}
