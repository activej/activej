package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import org.junit.Test;

import java.util.*;

import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConsolidationChunkSelectionTest {
	@Test
	public void testRangeExpansion() {
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong()).withKey("key", ofInt());
		AggregationState state = new AggregationState(structure);

		Set<AggregationChunk> chunks = new HashSet<>();
		chunks.add(createTestChunk(1, 1, 2));
		chunks.add(createTestChunk(2, 1, 2));
		chunks.add(createTestChunk(3, 1, 4));
		chunks.add(createTestChunk(4, 3, 4));
		chunks.add(createTestChunk(5, 3, 6));
		chunks.add(createTestChunk(6, 5, 6));
		chunks.add(createTestChunk(7, 5, 8));
		chunks.add(createTestChunk(8, 7, 8));

		state.apply(AggregationDiff.of(chunks));

		List<AggregationChunk> selectedChunks = state.findChunksForConsolidationHotSegment(100);
		assertEquals(chunks, new HashSet<>(selectedChunks));

		selectedChunks = state.findChunksForConsolidationHotSegment(5);
		assertEquals(5, selectedChunks.size());

		chunks.clear();
		chunks.add(createTestChunk(3, 1, 4));
		chunks.add(createTestChunk(4, 3, 4));
		chunks.add(createTestChunk(5, 3, 6));
		chunks.add(createTestChunk(6, 5, 6));
		chunks.add(createTestChunk(7, 5, 8));

		assertEquals(chunks, new HashSet<>(selectedChunks));
	}

	@Test
	public void testMinKeyStrategy() {
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong()).withKey("key", ofInt());
		AggregationState state = new AggregationState(structure);

		Set<AggregationChunk> chunks1 = new HashSet<>();
		chunks1.add(createTestChunk(1, 1, 2));
		chunks1.add(createTestChunk(2, 1, 2));
		chunks1.add(createTestChunk(3, 1, 4));
		chunks1.add(createTestChunk(4, 3, 4));

		Set<AggregationChunk> chunks2 = new HashSet<>();
		chunks2.add(createTestChunk(9, 9, 10));
		chunks2.add(createTestChunk(10, 9, 10));
		chunks2.add(createTestChunk(11, 10, 11));
		chunks2.add(createTestChunk(12, 10, 13));
		chunks2.add(createTestChunk(13, 12, 13));

		state.apply(AggregationDiff.of(concat(chunks1.stream(), chunks2.stream()).collect(toSet())));

		List<AggregationChunk> selectedChunks = state.findChunksForConsolidationMinKey(100, 4000);
		assertEquals(chunks1, new HashSet<>(selectedChunks));
	}

	@Test
	public void testSizeFixStrategy() {
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong()).withKey("key", ofInt());
		AggregationState state = new AggregationState(structure);

		int optimalChunkSize = 5;
		int maxChunks = 5;

		Set<AggregationChunk> chunks1 = new HashSet<>();
		chunks1.add(createTestChunk(1, 1, 2, optimalChunkSize));
		chunks1.add(createTestChunk(2, 3, 4, optimalChunkSize));

		Set<AggregationChunk> chunks2 = new HashSet<>();
		chunks2.add(createTestChunk(3, 5, 6, 4));
		chunks2.add(createTestChunk(4, 7, 8, 1));
		chunks2.add(createTestChunk(5, 9, 13, optimalChunkSize));
		chunks2.add(createTestChunk(6, 10, 11, optimalChunkSize));
		chunks2.add(createTestChunk(7, 10, 12, optimalChunkSize));

		Set<AggregationChunk> chunks3 = new HashSet<>();
		chunks3.add(createTestChunk(8, 14, 15, 3));
		chunks3.add(createTestChunk(9, 14, 15, 6));

		state.apply(AggregationDiff.of(concat(chunks1.stream(), concat(chunks2.stream(), chunks3.stream())).collect(toSet())));

		List<AggregationChunk> selectedChunks = state.findChunksForConsolidationMinKey(maxChunks, optimalChunkSize);
		assertEquals(chunks2, new HashSet<>(selectedChunks));
	}

	@Test
	public void testGroupingByPartition() {
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong()).withKey("key", ofInt());
		AggregationState state = new AggregationState(structure);

		Set<AggregationChunk> chunks1 = new HashSet<>();
		chunks1.add(createTestChunk(2, 1, 1, 1, 1, 1, 5));
		chunks1.add(createTestChunk(1, 1, 1, 1, 1, 1, 1));

		Set<AggregationChunk> chunks2 = new HashSet<>();
		chunks2.add(createTestChunk(3, 2, 2, 1, 1, 1, 1));
		chunks2.add(createTestChunk(4, 2, 2, 1, 1, 2, 2));

		Set<AggregationChunk> chunks3 = new HashSet<>();
		chunks3.add(createTestChunk(5, 2, 2, 2, 2, 3, 3));
		chunks3.add(createTestChunk(6, 2, 2, 2, 2, 1, 1));
		chunks3.add(createTestChunk(7, 2, 2, 2, 2, 1, 10));

		state.apply(AggregationDiff.of(concat(chunks1.stream(), concat(chunks2.stream(), chunks3.stream())).collect(toSet())));

		Map<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> partitioningKeyToTree = state.groupByPartition(2);

		assert partitioningKeyToTree != null;
		assertEquals(chunks1, partitioningKeyToTree.get(PrimaryKey.ofArray(1, 1)).getAll());
		assertEquals(chunks2, partitioningKeyToTree.get(PrimaryKey.ofArray(2, 1)).getAll());
		assertEquals(chunks3, partitioningKeyToTree.get(PrimaryKey.ofArray(2, 2)).getAll());

		state.addToIndex(createTestChunk(8, 1, 1, 2, 3, 5, 5));
		assertNull(state.groupByPartition(2));
	}

	private static AggregationChunk createTestChunk(int id, int min, int max) {
		return createTestChunk(id, min, max, id);
	}

	private static AggregationChunk createTestChunk(int id, int d1Min, int d1Max, int d2Min, int d2Max, int d3Min, int d3Max) {
		return AggregationChunk.create(id, new ArrayList<>(), PrimaryKey.ofArray(d1Min, d2Min, d3Min),
				PrimaryKey.ofArray(d1Max, d2Max, d3Max), 10);
	}

	private static AggregationChunk createTestChunk(int id, int min, int max, int count) {
		return AggregationChunk.create(id, new ArrayList<>(), PrimaryKey.ofArray(min), PrimaryKey.ofArray(max), count);
	}
}
