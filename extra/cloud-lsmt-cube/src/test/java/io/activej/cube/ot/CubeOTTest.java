package io.activej.cube.ot;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogOTState;
import io.activej.etl.LogPositionDiff;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.TransformResult;
import io.activej.ot.TransformResult.ConflictResolution;
import io.activej.ot.exception.TransformException;
import io.activej.ot.system.OTSystem;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static io.activej.aggregation.PrimaryKey.ofArray;
import static io.activej.common.Utils.mapOf;
import static io.activej.cube.TestUtils.STUB_CUBE_STATE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
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
		return cubeDiff(asList(added), Collections.emptyList());
	}

	private static CubeDiff cubeDiff(List<AggregationChunk> added, List<AggregationChunk> removed) {
		return CubeDiff.of(singletonMap("key", AggregationDiff.of(new HashSet<>(added),
				new HashSet<>(removed))));
	}

	private static AggregationChunk chunk(List<String> fields, PrimaryKey minKey, PrimaryKey maxKey, int count) {
		return AggregationChunk.create((long) 1, fields, minKey, maxKey, count);
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
		List<String> fields = asList("field1", "field2");
		LogDiff<CubeDiff> changesLeft = LogDiff.of(
				singletonMap("clicks", positionDiff(logFile, 0, 10)),
				cubeDiff(chunk(fields, ofArray("str", 10), ofArray("str", 20), 15)));

		LogDiff<CubeDiff> changesRight = LogDiff.of(
				singletonMap("clicks", positionDiff(logFile, 0, 20)),
				cubeDiff(chunk(fields, ofArray("str", 10), ofArray("str", 25), 30)));
		TransformResult<LogDiff<CubeDiff>> transform = logSystem.transform(changesLeft, changesRight);

		assertTrue(transform.hasConflict());
		assertEquals(ConflictResolution.RIGHT, transform.resolution);
		assertThat(transform.right, IsEmptyCollection.empty());

		LogDiff<CubeDiff> result = LogDiff.of(
				singletonMap("clicks", positionDiff(logFile, 10, 20)),
				cubeDiff(addedChunks(changesRight.getDiffs()), addedChunks(changesLeft.getDiffs())));

		assertEquals(1, transform.left.size());
		assertEquals(result, transform.left.get(0));
	}

	@Test
	public void testInversion() {
		LogOTState<CubeDiff> state = LogOTState.create(STUB_CUBE_STATE);
		state.init();

		assertTrue(state.getPositions().isEmpty());

		LogPosition fromPosition = LogPosition.initial();
		LogFile toLogfile = new LogFile("myLog", 100);
		LogPosition toPosition = LogPosition.create(toLogfile, 500);

		LogDiff<CubeDiff> logDiff = LogDiff.of(mapOf("test", new LogPositionDiff(fromPosition, toPosition)), CubeDiff.empty());

		state.apply(logDiff);
		Map<String, LogPosition> positions = state.getPositions();
		assertEquals(mapOf("test", toPosition), positions);

		List<LogDiff<CubeDiff>> inverted = logSystem.invert(singletonList(logDiff));
		for (LogDiff<CubeDiff> invertedDiff : inverted) {
			state.apply(invertedDiff);
		}

		assertTrue(state.getPositions().isEmpty());
	}
}
