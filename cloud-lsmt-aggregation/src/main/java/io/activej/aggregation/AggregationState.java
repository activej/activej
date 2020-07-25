/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.aggregation;

import io.activej.aggregation.AggregationPredicates.RangeScan;
import io.activej.aggregation.RangeTree.Segment;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.ot.OTState;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.activej.aggregation.AggregationPredicates.toRangeScan;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.intersection;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;

/**
 * Represents aggregation metadata. Stores chunks in an index (represented by an array of {@link RangeTree}) for efficient search.
 * Provides methods for managing index, querying for chunks by key, searching for chunks that are available for consolidation.
 */
public final class AggregationState implements OTState<AggregationDiff> {
	private static final Logger logger = LoggerFactory.getLogger(AggregationState.class);

	private final AggregationStructure aggregation;

	private final Map<Object, AggregationChunk> chunks = new LinkedHashMap<>();
	private RangeTree<PrimaryKey, AggregationChunk>[] prefixRanges;

	private static final Comparator<AggregationChunk> MIN_KEY_ASCENDING_COMPARATOR = Comparator.comparing(AggregationChunk::getMinPrimaryKey);

	AggregationState(AggregationStructure aggregation) {
		this.aggregation = aggregation;
		initIndex();
	}

	public Map<Object, AggregationChunk> getChunks() {
		return unmodifiableMap(chunks);
	}

	@Override
	public void apply(AggregationDiff commit) {
		checkArgument(intersection(commit.getAddedChunks(), commit.getRemovedChunks()), Set::isEmpty,
				v -> "Non-empty intersection between added and removed chunks: " + v +
						"\n Added chunks " + toLimitedString(commit.getAddedChunks(), 100) +
						"\n Removed chunks intersection: " + toLimitedString(commit.getRemovedChunks(), 100));

		for (AggregationChunk chunk : commit.getAddedChunks()) {
			addToIndex(chunk);
		}

		for (AggregationChunk chunk : commit.getRemovedChunks()) {
			removeFromIndex(chunk);
		}
	}

	public void addToIndex(AggregationChunk chunk) {
		checkArgument(chunks.put(chunk.getChunkId(), chunk) == null,
				() -> "" +
						"Trying to add existing chunk: " + chunk +
						"\n this: " + toString() +
						"\n chunks: " + toLimitedString(chunks.keySet(), 100));

		for (int size = 0; size <= aggregation.getKeys().size(); size++) {
			RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];

			PrimaryKey lower = chunk.getMinPrimaryKey().prefix(size);
			PrimaryKey upper = chunk.getMaxPrimaryKey().prefix(size);
			index.put(lower, upper, chunk);
		}
	}

	public void removeFromIndex(AggregationChunk chunk) {
		checkArgument(chunks.remove(chunk.getChunkId()) != null,
				() -> "" +
						"Trying to remove unknown chunk: " + chunk +
						"\n this: " + toString() +
						"\n chunks: " + toLimitedString(chunks.keySet(), 100));

		for (int size = 0; size <= aggregation.getKeys().size(); size++) {
			RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];

			PrimaryKey lower = chunk.getMinPrimaryKey().prefix(size);
			PrimaryKey upper = chunk.getMaxPrimaryKey().prefix(size);
			index.remove(lower, upper, chunk);
		}
	}

	@SuppressWarnings("unchecked")
	void initIndex() {
		prefixRanges = new RangeTree[aggregation.getKeys().size() + 1];
		for (int size = 0; size <= aggregation.getKeys().size(); size++) {
			prefixRanges[size] = RangeTree.create();
		}
	}

	@Override
	public void init() {
		initIndex();
		chunks.clear();
	}

	private static int getNumberOfOverlaps(Segment<?> segment) {
		return segment.getSet().size() + segment.getClosingSet().size();
	}

	public Set<AggregationChunk> findOverlappingChunks() {
		int minOverlaps = 2;
		Set<AggregationChunk> result = new HashSet<>();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[aggregation.getKeys().size()];
		for (Map.Entry<PrimaryKey, Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = getNumberOfOverlaps(segment);
			if (overlaps >= minOverlaps) {
				result.addAll(segment.getSet());
				result.addAll(segment.getClosingSet());
			}
		}
		return result;
	}

	public List<AggregationChunk> findChunksGroupWithMostOverlaps() {
		return findChunksGroupWithMostOverlaps(prefixRanges[aggregation.getKeys().size()]);
	}

	private static List<AggregationChunk> findChunksGroupWithMostOverlaps(RangeTree<PrimaryKey, AggregationChunk> tree) {
		int maxOverlaps = 2;
		List<AggregationChunk> result = new ArrayList<>();
		for (Map.Entry<PrimaryKey, Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = getNumberOfOverlaps(segment);
			if (overlaps >= maxOverlaps) {
				maxOverlaps = overlaps;
				result.clear();
				result.addAll(segment.getSet());
				result.addAll(segment.getClosingSet());
			}
		}
		return result;
	}

	private static PickedChunks findChunksWithMinKeyOrSizeFixStrategy(SortedMap<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> partitioningKeyToTree,
			int maxChunks, int optimalChunkSize) {
		int minChunks = 2;
		for (Map.Entry<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> entry : partitioningKeyToTree.entrySet()) {
			ChunksAndStrategy chunksAndStrategy = findChunksWithMinKeyOrSizeFixStrategy(entry.getValue(), maxChunks, optimalChunkSize);
			if (chunksAndStrategy.chunks.size() >= minChunks)
				return new PickedChunks(chunksAndStrategy.strategy, entry.getValue(), chunksAndStrategy.chunks);
		}
		return new PickedChunks(PickingStrategy.MIN_KEY, null, emptyList());
	}

	private static ChunksAndStrategy findChunksWithMinKeyOrSizeFixStrategy(RangeTree<PrimaryKey, AggregationChunk> tree,
			int maxChunks, int optimalChunkSize) {
		int minOverlaps = 2;
		List<AggregationChunk> result = new ArrayList<>();
		SortedMap<PrimaryKey, Segment<AggregationChunk>> tailMap = null;
		for (Map.Entry<PrimaryKey, Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = getNumberOfOverlaps(segment);

			// "min key" strategy
			if (overlaps >= minOverlaps) {
				result.addAll(segment.getSet());
				result.addAll(segment.getClosingSet());
				return new ChunksAndStrategy(PickingStrategy.MIN_KEY, result);
			}

			List<AggregationChunk> segmentChunks = new ArrayList<>();
			segmentChunks.addAll(segment.getSet());
			segmentChunks.addAll(segment.getClosingSet());

			// "size fix" strategy
			if (overlaps == 1 && segmentChunks.get(0).getCount() != optimalChunkSize) {
				tailMap = tree.getSegments().tailMap(segmentEntry.getKey());
				break;
			}
		}

		if (tailMap == null)
			return new ChunksAndStrategy(PickingStrategy.SIZE_FIX, emptyList());

		Set<AggregationChunk> chunks = new HashSet<>();
		for (Map.Entry<PrimaryKey, Segment<AggregationChunk>> segmentEntry : tailMap.entrySet()) {
			if (chunks.size() >= maxChunks)
				break;

			Segment<AggregationChunk> segment = segmentEntry.getValue();
			chunks.addAll(segment.getSet());
			chunks.addAll(segment.getClosingSet());
		}
		result.addAll(chunks);

		if (result.size() == 1) {
			if (result.get(0).getCount() > optimalChunkSize)
				return new ChunksAndStrategy(PickingStrategy.SIZE_FIX, result);
			else
				return new ChunksAndStrategy(PickingStrategy.SIZE_FIX, emptyList());
		}

		return new ChunksAndStrategy(PickingStrategy.SIZE_FIX, result);
	}

	private enum PickingStrategy {
		PARTITIONING,
		HOT_SEGMENT,
		MIN_KEY,
		SIZE_FIX
	}

	private static class PickedChunks {
		private final PickingStrategy strategy;
		@Nullable
		private final RangeTree<PrimaryKey, AggregationChunk> partitionTree;
		private final List<AggregationChunk> chunks;

		public PickedChunks(PickingStrategy strategy, @Nullable RangeTree<PrimaryKey, AggregationChunk> partitionTree,
				List<AggregationChunk> chunks) {
			this.strategy = strategy;
			this.partitionTree = partitionTree;
			this.chunks = chunks;
		}
	}

	private static class ChunksAndStrategy {
		private final PickingStrategy strategy;
		private final List<AggregationChunk> chunks;

		public ChunksAndStrategy(PickingStrategy strategy, List<AggregationChunk> chunks) {
			this.strategy = strategy;
			this.chunks = chunks;
		}
	}

	// visibleForTest
	SortedMap<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> groupByPartition(int partitioningKeyLength) {
		SortedMap<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> partitioningKeyToTree = new TreeMap<>();

		Set<AggregationChunk> allChunks = prefixRanges[0].getAll();
		for (AggregationChunk chunk : allChunks) {
			PrimaryKey minKeyPrefix = chunk.getMinPrimaryKey().prefix(partitioningKeyLength);
			PrimaryKey maxKeyPrefix = chunk.getMaxPrimaryKey().prefix(partitioningKeyLength);

			if (!minKeyPrefix.equals(maxKeyPrefix))
				return null; // not partitioned

			partitioningKeyToTree.computeIfAbsent(minKeyPrefix, $ -> RangeTree.create())
					.put(chunk.getMinPrimaryKey(), chunk.getMaxPrimaryKey(), chunk);

		}

		return partitioningKeyToTree;
	}

	private List<AggregationChunk> findChunksForPartitioning(int partitioningKeyLength, int maxChunks) {
		List<AggregationChunk> chunksForPartitioning = new ArrayList<>();
		List<AggregationChunk> allChunks = new ArrayList<>(prefixRanges[0].getAll());
		allChunks.sort(MIN_KEY_ASCENDING_COMPARATOR);

		for (AggregationChunk chunk : allChunks) {
			if (chunksForPartitioning.size() == maxChunks)
				break;

			PrimaryKey minKeyPrefix = chunk.getMinPrimaryKey().prefix(partitioningKeyLength);
			PrimaryKey maxKeyPrefix = chunk.getMaxPrimaryKey().prefix(partitioningKeyLength);

			if (!minKeyPrefix.equals(maxKeyPrefix))
				chunksForPartitioning.add(chunk);
		}

		return chunksForPartitioning;
	}

	public List<AggregationChunk> findChunksForConsolidationMinKey(int maxChunks, int optimalChunkSize) {
		int partitioningKeyLength = aggregation.getPartitioningKey().size();
		SortedMap<PrimaryKey, RangeTree<PrimaryKey, AggregationChunk>> partitioningKeyToTree = groupByPartition(partitioningKeyLength);
		if (partitioningKeyToTree == null) { // not partitioned
			List<AggregationChunk> chunks = findChunksForPartitioning(partitioningKeyLength, maxChunks);
			logChunksAndStrategy(chunks, PickingStrategy.PARTITIONING);
			return chunks; // launch partitioning
		}
		PickedChunks pickedChunks = findChunksWithMinKeyOrSizeFixStrategy(partitioningKeyToTree, maxChunks, optimalChunkSize);
		return processSelection(pickedChunks.chunks, maxChunks, pickedChunks.partitionTree, pickedChunks.strategy);
	}

	public List<AggregationChunk> findChunksForConsolidationHotSegment(int maxChunks) {
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[aggregation.getKeys().size()];
		List<AggregationChunk> chunks = findChunksGroupWithMostOverlaps(tree);
		return processSelection(chunks, maxChunks, tree, PickingStrategy.HOT_SEGMENT);
	}

	private static List<AggregationChunk> processSelection(List<AggregationChunk> chunks, int maxChunks,
			RangeTree<PrimaryKey, AggregationChunk> partitionTree,
			PickingStrategy strategy) {
		if (chunks.isEmpty() || chunks.size() == maxChunks) {
			logChunksAndStrategy(chunks, strategy);
			return chunks;
		}

		if (chunks.size() > maxChunks) {
			List<AggregationChunk> trimmedChunks = trimChunks(chunks, maxChunks);
			logChunksAndStrategy(trimmedChunks, strategy);
			return trimmedChunks;
		}

		if (strategy == PickingStrategy.SIZE_FIX) {
			logChunksAndStrategy(chunks, PickingStrategy.SIZE_FIX);
			return chunks;
		}

		List<AggregationChunk> expandedChunks = expandRange(partitionTree, chunks, maxChunks);

		if (expandedChunks.size() > maxChunks) {
			List<AggregationChunk> trimmedChunks = trimChunks(expandedChunks, maxChunks);
			logChunksAndStrategy(trimmedChunks, strategy);
			return trimmedChunks;
		}

		logChunksAndStrategy(expandedChunks, strategy);
		return expandedChunks;
	}

	private static void logChunksAndStrategy(Collection<AggregationChunk> chunks, PickingStrategy strategy) {
		if (logger.isInfoEnabled()) {
			String chunkIds = chunks.stream()
					.map(AggregationChunk::getChunkId)
					.map(Object::toString)
					.collect(Collectors.joining(",", "[", "]"));
			logger.info("Chunks for consolidation {}: {}. Strategy: {}", chunks.size(), chunkIds, strategy);
		}
	}

	private static List<AggregationChunk> trimChunks(List<AggregationChunk> chunks, int maxChunks) {
		chunks.sort(MIN_KEY_ASCENDING_COMPARATOR);
		return chunks.subList(0, maxChunks);
	}

	private static boolean expandRange(RangeTree<PrimaryKey, AggregationChunk> tree, Set<AggregationChunk> chunks) {
		PrimaryKey minKey = null;
		PrimaryKey maxKey = null;

		for (AggregationChunk chunk : chunks) {
			PrimaryKey chunkMinKey = chunk.getMinPrimaryKey();
			PrimaryKey chunkMaxKey = chunk.getMaxPrimaryKey();

			if (minKey == null) {
				minKey = chunkMinKey;
				maxKey = chunkMaxKey;
				continue;
			}

			if (chunkMinKey.compareTo(minKey) < 0)
				minKey = chunkMinKey;

			if (chunkMaxKey.compareTo(maxKey) > 0)
				maxKey = chunkMaxKey;
		}

		Set<AggregationChunk> chunksForRange = tree.getRange(minKey, maxKey);
		return chunks.addAll(chunksForRange);
	}

	private static void expandRange(RangeTree<PrimaryKey, AggregationChunk> tree, Set<AggregationChunk> chunks, int maxChunks) {
		boolean expand = chunks.size() < maxChunks;

		while (expand) {
			boolean expanded = expandRange(tree, chunks);
			expand = expanded && chunks.size() < maxChunks;
		}
	}

	private static List<AggregationChunk> expandRange(RangeTree<PrimaryKey, AggregationChunk> tree,
			List<AggregationChunk> chunks, int maxChunks) {
		Set<AggregationChunk> chunkSet = new HashSet<>(chunks);
		expandRange(tree, chunkSet, maxChunks);
		return new ArrayList<>(chunkSet);
	}

	public List<ConsolidationDebugInfo> getConsolidationDebugInfo() {
		List<ConsolidationDebugInfo> infos = new ArrayList<>();
		RangeTree<PrimaryKey, AggregationChunk> tree = prefixRanges[aggregation.getKeys().size()];

		for (Map.Entry<PrimaryKey, Segment<AggregationChunk>> segmentEntry : tree.getSegments().entrySet()) {
			PrimaryKey key = segmentEntry.getKey();
			Segment<AggregationChunk> segment = segmentEntry.getValue();
			int overlaps = segment.getSet().size() + segment.getClosingSet().size();
			Set<AggregationChunk> segmentSet = segment.getSet();
			Set<AggregationChunk> segmentClosingSet = segment.getClosingSet();
			infos.add(new ConsolidationDebugInfo(key, segmentSet, segmentClosingSet, overlaps));
		}

		return infos;
	}

	public static class ConsolidationDebugInfo {
		public final PrimaryKey key;
		public final Set<AggregationChunk> segmentSet;
		public final Set<AggregationChunk> segmentClosingSet;
		public final int overlaps;

		public ConsolidationDebugInfo(PrimaryKey key, Set<AggregationChunk> segmentSet,
				Set<AggregationChunk> segmentClosingSet, int overlaps) {
			this.key = key;
			this.segmentSet = segmentSet;
			this.segmentClosingSet = segmentClosingSet;
			this.overlaps = overlaps;
		}
	}

	// visibleForTesting
	public static boolean chunkMightContainQueryValues(PrimaryKey minQueryKey, PrimaryKey maxQueryKey,
			PrimaryKey minChunkKey, PrimaryKey maxChunkKey) {
		return chunkMightContainQueryValues(minQueryKey.values(), maxQueryKey.values(),
				minChunkKey.values(), maxChunkKey.values());
	}

	@SuppressWarnings("unchecked")
	private static boolean chunkMightContainQueryValues(List<Object> queryMinValues, List<Object> queryMaxValues,
			List<Object> chunkMinValues, List<Object> chunkMaxValues) {
		checkArgument(queryMinValues.size() == queryMaxValues.size(),
				"Sizes of lists of query minimum and maximum values should match");
		checkArgument(chunkMinValues.size() == chunkMaxValues.size(),
				"Sizes of lists of chunk minimum and maximum values should match");

		for (int i = 0; i < queryMinValues.size(); ++i) {
			Comparable<Object> queryMinValue = (Comparable<Object>) queryMinValues.get(i);
			Comparable<Object> queryMaxValue = (Comparable<Object>) queryMaxValues.get(i);
			Comparable<Object> chunkMinValue = (Comparable<Object>) chunkMinValues.get(i);
			Comparable<Object> chunkMaxValue = (Comparable<Object>) chunkMaxValues.get(i);

			if (chunkMinValue.compareTo(chunkMaxValue) == 0) {
				if (!(queryMinValue.compareTo(chunkMinValue) <= 0 && queryMaxValue.compareTo(chunkMaxValue) >= 0)) {
					return false;
				}
			} else {
				return queryMinValue.compareTo(chunkMaxValue) <= 0 && queryMaxValue.compareTo(chunkMinValue) >= 0;
			}
		}

		return true;
	}

	public List<AggregationChunk> findChunks(AggregationPredicate predicate, List<String> fields) {
		Set<String> requestedFields = new HashSet<>(fields);

		RangeScan rangeScan = toRangeScan(predicate, aggregation.getKeys(), aggregation.getKeyTypes());

		List<AggregationChunk> chunks = new ArrayList<>();
		for (AggregationChunk chunk : rangeQuery(rangeScan.getFrom(), rangeScan.getTo())) {
			if (intersection(new HashSet<>(chunk.getMeasures()), requestedFields).isEmpty())
				continue;

			chunks.add(chunk);
		}

		return chunks;
	}

	private List<AggregationChunk> rangeQuery(PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey) {
		checkArgument(minPrimaryKey.size() == maxPrimaryKey.size(), "Sizes of min primary key and max primary key should match");
		int size = minPrimaryKey.size();
		RangeTree<PrimaryKey, AggregationChunk> index = prefixRanges[size];
		return new ArrayList<>(index.getRange(minPrimaryKey, maxPrimaryKey));
	}

	@Override
	public String toString() {
		return "Aggregation{keys=" + aggregation.getKeys() + ", fields=" + aggregation.getMeasures() + '}';
	}
}
