package io.activej.cube.ot;

import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogPositionDiff;
import io.activej.etl.LogState;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.TransformResult;
import io.activej.ot.TransformResult.ConflictResolution;
import io.activej.ot.exception.TransformException;
import io.activej.ot.system.OTSystem;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.activej.cube.TestUtils.STUB_CUBE_STATE;
import static io.activej.cube.aggregation.PrimaryKey.ofArray;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CubeOTTest {
	private OTSystem<LogDiff<CubeDiff>> logSystem;

	@Before
	public void before() {
		logSystem = LogOT.createLogOT(CubeOT.createCubeOT());
	}

	private static LogPositionDiff positionDiff(LogFile logFile, long start, long end) {
		return new LogPositionDiff(LogPosition.create(logFile, start), LogPosition.create(logFile, end));
	}

	private static CubeDiff cubeDiff(AggregationChunk... added) {
		return cubeDiff(List.of(added), List.of());
	}

	private static CubeDiff cubeDiff(List<AggregationChunk> added, List<AggregationChunk> removed) {
		return CubeDiff.of(Map.of(
			"key", AggregationDiff.of(new HashSet<>(added), new HashSet<>(removed))));
	}

	private static List<AggregationChunk> addedChunks(Collection<CubeDiff> cubeDiffs) {
		return cubeDiffs.stream()
			.flatMap(cubeDiff -> cubeDiff.keySet().stream().map(cubeDiff::get))
			.map(AggregationDiff::getAddedChunks)
			.flatMap(Collection::stream)
			.collect(toList());
	}

	@Test
	public void test() throws TransformException {
		LogFile logFile = new LogFile("file", 1);
		List<String> fields = List.of("field1", "field2");
		LogDiff<CubeDiff> changesLeft = LogDiff.of(Map.of(
				"clicks", positionDiff(logFile, 0, 10)),
			cubeDiff(AggregationChunk.create(1, fields, ofArray("str", 10), ofArray("str", 20), 15)));

		LogDiff<CubeDiff> changesRight = LogDiff.of(Map.of(
				"clicks", positionDiff(logFile, 0, 20)),
			cubeDiff(AggregationChunk.create(2, fields, ofArray("str", 10), ofArray("str", 25), 30)));
		TransformResult<LogDiff<CubeDiff>> transform = logSystem.transform(changesLeft, changesRight);

		assertTrue(transform.hasConflict());
		assertEquals(ConflictResolution.RIGHT, transform.resolution);
		assertTrue(transform.right.isEmpty());

		LogDiff<CubeDiff> result = LogDiff.of(Map.of(
				"clicks", positionDiff(logFile, 10, 20)),
			cubeDiff(addedChunks(changesRight.getDiffs()), addedChunks(changesLeft.getDiffs())));

		assertEquals(1, transform.left.size());
		assertEquals(result, transform.left.get(0));
	}

	@Test
	public void testInversion() {
		LogState<CubeDiff, ?> state = LogState.create(STUB_CUBE_STATE);
		state.init();

		assertTrue(state.getPositions().isEmpty());

		LogPosition fromPosition = LogPosition.initial();
		LogFile toLogfile = new LogFile("myLog", 100);
		LogPosition toPosition = LogPosition.create(toLogfile, 500);

		LogDiff<CubeDiff> logDiff = LogDiff.of(Map.of(
				"test", new LogPositionDiff(fromPosition, toPosition)),
			CubeDiff.empty());

		state.apply(logDiff);
		Map<String, LogPosition> positions = state.getPositions();
		assertEquals(Map.of(
				"test", toPosition),
			positions);

		List<LogDiff<CubeDiff>> inverted = logSystem.invert(List.of(logDiff));
		for (LogDiff<CubeDiff> invertedDiff : inverted) {
			state.apply(invertedDiff);
		}

		assertTrue(state.getPositions().isEmpty());
	}
}
