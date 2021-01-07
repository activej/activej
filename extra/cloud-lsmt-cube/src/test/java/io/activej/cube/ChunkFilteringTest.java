package io.activej.cube;

import io.activej.aggregation.AggregationState;
import io.activej.aggregation.PrimaryKey;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChunkFilteringTest {
	@Test
	public void testChunkMightContainQueryValues1() {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(0, 3, 78, 10);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(10, 8, 79, 12);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 77, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 22);

		assertTrue(AggregationState.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}

	@Test
	public void testChunkMightContainQueryValues2() {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(3, 5, 81, 10);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(3, 5, 85, 12);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 77, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 22);

		assertFalse(AggregationState.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}

	@Test
	public void testChunkMightContainQueryValues3() {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(14, 5, 78, 10);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(20, 5, 79, 12);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 77, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 22);

		assertFalse(AggregationState.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}

	@Test
	public void testChunkMightContainQueryValues4() {
		PrimaryKey minQueryKey = PrimaryKey.ofArray(3, 5, 80, 90);
		PrimaryKey maxQueryKey = PrimaryKey.ofArray(3, 5, 80, 90);

		PrimaryKey minChunkKey = PrimaryKey.ofArray(3, 5, 80, 90);
		PrimaryKey maxChunkKey = PrimaryKey.ofArray(3, 5, 80, 90);

		assertTrue(AggregationState.chunkMightContainQueryValues(minQueryKey, maxQueryKey, minChunkKey, maxChunkKey));
	}
}
