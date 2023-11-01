package io.activej.cube;

import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.cube.aggregation.fieldtype.FieldTypes;
import io.activej.cube.aggregation.measure.Measures;
import io.activej.cube.aggregation.ot.AggregationDiff;
import org.junit.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.activej.cube.TestUtils.aggregationStructureBuilder;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.*;
import static org.junit.Assert.assertEquals;

public class AggregationStateTest {
	private static final LocalDate START_DATE = LocalDate.now();
	private static final String MEASURE = "measure";
	private static final List<String> MEASURES_LIST = List.of(MEASURE);

	@Test
	public void testFindChunksWithNoPrefixPredicate() {
		AggregationStructure structure = aggregationStructureBuilder()
			.withMeasure(MEASURE, Measures.sum(FieldTypes.ofInt()))
			.withKey("date", FieldTypes.ofLocalDate(START_DATE))
			.withKey("network", FieldTypes.ofInt())
			.withKey("advertiser", FieldTypes.ofInt())
			.build();

		AggregationState aggregationState = new AggregationState(structure);
		aggregationState.init();
		aggregationState.apply(
			AggregationDiff.of(Set.of(
				AggregationChunk.create(1L, MEASURES_LIST, PrimaryKey.ofArray(100, 1, 1), PrimaryKey.ofArray(100, 1, 2), 10),
				AggregationChunk.create(2L, MEASURES_LIST, PrimaryKey.ofArray(200, 2, 2), PrimaryKey.ofArray(200, 3, 2), 10),
				AggregationChunk.create(3L, MEASURES_LIST, PrimaryKey.ofArray(300, 3, 3), PrimaryKey.ofArray(300, 4, 4), 10)
			))
		);

		List<AggregationChunk> chunks = aggregationState.findChunks(MEASURES_LIST, and(eq("network", 4), ge("advertiser", 2)));

		assertEquals(Set.of(3L), chunks.stream().map(AggregationChunk::getChunkId).collect(Collectors.toSet()));
	}

	@Test
	public void testFindChunksWithNoPrefixPredicateReverseKeyOrder() {
		AggregationStructure structure = aggregationStructureBuilder()
			.withMeasure(MEASURE, Measures.sum(FieldTypes.ofInt()))
			.withKey("advertiser", FieldTypes.ofInt())
			.withKey("network", FieldTypes.ofInt())
			.withKey("date", FieldTypes.ofLocalDate(START_DATE))
			.build();

		AggregationState aggregationState = new AggregationState(structure);
		aggregationState.init();
		aggregationState.apply(
			AggregationDiff.of(Set.of(
				AggregationChunk.create(1L, MEASURES_LIST, PrimaryKey.ofArray(1, 1, 100), PrimaryKey.ofArray(1, 1, 200), 10),
				AggregationChunk.create(2L, MEASURES_LIST, PrimaryKey.ofArray(2, 2, 200), PrimaryKey.ofArray(2, 3, 200), 10),
				AggregationChunk.create(3L, MEASURES_LIST, PrimaryKey.ofArray(3, 3, 300), PrimaryKey.ofArray(3, 4, 400), 10)
			))
		);

		List<AggregationChunk> chunks = aggregationState.findChunks(MEASURES_LIST, and(eq("network", 4), ge("date", START_DATE.plusDays(200))));

		assertEquals(Set.of(3L), chunks.stream().map(AggregationChunk::getChunkId).collect(Collectors.toSet()));
	}
}
