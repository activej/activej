package io.activej.cube;

import io.activej.common.ref.RefLong;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.ot.StateManager;
import io.activej.ot.StateManager.StateChangesSupplier;
import io.activej.promise.Promises;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class CubeStateChangesTest extends CubeTestBase {
	public static final String AGGREGATION_ID = "aggregation";

	private final CubeStructure structure = CubeStructure.builder()
		.withDimension("key", ofInt())
		.withMeasure("value", sum(ofInt()))
		.withMeasure("timestamp", sum(ofLong()))
		.withAggregation(CubeStructure.AggregationConfig.id(AGGREGATION_ID)
			.withDimensions("key")
			.withMeasures("value", "timestamp")
		)
		.build();

	private StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		stateManager = stateManagerFactory.create(structure, description);
	}

	@Test
	public void testStateChangesSequential() {
		StateChangesSupplier<LogDiff<CubeDiff>> stateChangesSupplier = stateManager.subscribeToStateChanges();

		long chunkId = 0;
		for (int i = 0; i < 10; i++) {
			CubeDiff cubeDiff = CubeDiff.of(Map.of(AGGREGATION_ID, AggregationDiff.of(idsToChunks(Set.of(++chunkId, ++chunkId, ++chunkId)), Set.of())));
			LogDiff<CubeDiff> diff = LogDiff.forCurrentPosition(cubeDiff);
			stateManager.push(List.of(diff));
			LogDiff<CubeDiff> logDiff = await(stateChangesSupplier.get());
			assertEquals(diff, logDiff);
		}
	}

	@Test
	public void testStateChanges() {
		StateChangesSupplier<LogDiff<CubeDiff>> stateChangesSupplier = stateManager.subscribeToStateChanges();

		List<LogDiff<CubeDiff>> expected = new ArrayList<>();
		RefLong chunkIdRef = new RefLong(0);

		await(Promises.sequence(IntStream.range(0, 10)
			.mapToObj($ -> () -> {
				CubeDiff cubeDiff = CubeDiff.of(Map.of(AGGREGATION_ID, AggregationDiff.of(idsToChunks(Set.of(chunkIdRef.inc(), chunkIdRef.inc(), chunkIdRef.inc())), Set.of())));
				LogDiff<CubeDiff> diff = LogDiff.forCurrentPosition(cubeDiff);
				expected.add(diff);
				return stateManager.push(List.of(diff));
			}))
			.then(() -> Promises.sequence(expected.stream()
				.map(diff -> () -> stateChangesSupplier.get()
					.whenResult(logDiff -> assertEquals(diff, logDiff))
					.toVoid()))));
	}

	private static Set<AggregationChunk> idsToChunks(Set<Long> fromChunks) {
		return fromChunks.stream()
			.map(chunkId -> AggregationChunk.create(chunkId,
				List.of("value", "timestamp"),
				PrimaryKey.ofList(List.of(0)),
				PrimaryKey.ofList(List.of(1)),
				10))
			.collect(Collectors.toSet());
	}
}
