package io.activej.aggregation;

import io.activej.aggregation.fieldtype.FieldTypes;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import org.junit.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import static io.activej.aggregation.AggregationPredicates.*;
import static io.activej.common.Utils.setOf;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class AggregationStateTest {
	private static final LocalDate START_DATE = LocalDate.now();
	private static final List<String> MEASURE = singletonList("measure");

	@Test
	public void testFindChunksWithNoPrefixPredicate() {
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withKey("date", FieldTypes.ofLocalDate(START_DATE))
				.withKey("network", FieldTypes.ofInt())
				.withKey("advertiser", FieldTypes.ofInt());

		AggregationState aggregationState = new AggregationState(structure);
		aggregationState.init();
		aggregationState.apply(
				AggregationDiff.of(setOf(
						AggregationChunk.create(1L, MEASURE, PrimaryKey.ofArray(100, 1, 1), PrimaryKey.ofArray(100, 1, 2), 10),
						AggregationChunk.create(2L, MEASURE, PrimaryKey.ofArray(200, 2, 2), PrimaryKey.ofArray(200, 3, 2), 10),
						AggregationChunk.create(3L, MEASURE, PrimaryKey.ofArray(300, 3, 3), PrimaryKey.ofArray(300, 4, 4), 10)
				))
		);

		List<AggregationChunk> chunks = aggregationState.findChunks(and(eq("network", 4), ge("advertiser", 2)), MEASURE);

		assertEquals(singleton(3L), chunks.stream().map(AggregationChunk::getChunkId).collect(Collectors.toSet()));
	}

	@Test
	public void testFindChunksWithNoPrefixPredicateReverseKeyOrder() {
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withKey("advertiser", FieldTypes.ofInt())
				.withKey("network", FieldTypes.ofInt())
				.withKey("date", FieldTypes.ofLocalDate(START_DATE));

		AggregationState aggregationState = new AggregationState(structure);
		aggregationState.init();
		aggregationState.apply(
				AggregationDiff.of(setOf(
						AggregationChunk.create(1L, MEASURE, PrimaryKey.ofArray(1, 1, 100), PrimaryKey.ofArray(1, 1, 200), 10),
						AggregationChunk.create(2L, MEASURE, PrimaryKey.ofArray(2, 2, 200), PrimaryKey.ofArray(2, 3, 200), 10),
						AggregationChunk.create(3L, MEASURE, PrimaryKey.ofArray(3, 3, 300), PrimaryKey.ofArray(3, 4, 400), 10)
				))
		);

		List<AggregationChunk> chunks = aggregationState.findChunks(and(eq("network", 4), ge("date", START_DATE.plusDays(200))), MEASURE);

		assertEquals(singleton(3L), chunks.stream().map(AggregationChunk::getChunkId).collect(Collectors.toSet()));
	}
}
