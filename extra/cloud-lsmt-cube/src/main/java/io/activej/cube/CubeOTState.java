package io.activej.cube;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.AggregationOTState;
import io.activej.aggregation.AggregationOTState.ConsolidationDebugInfo;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.aggregation.predicate.AggregationPredicate;
import io.activej.cube.ot.CubeDiff;
import io.activej.ot.OTState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.activej.aggregation.predicate.AggregationPredicates.*;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toList;

public final class CubeOTState implements OTState<CubeDiff> {
	private static final Logger logger = LoggerFactory.getLogger(CubeOTState.class);

	private final CubeStructure cubeStructure;
	private final Map<String, AggregationOTState> aggregationOTStates;

	private CubeOTState(CubeStructure cubeStructure, Map<String, AggregationOTState> aggregationOTStates) {
		this.cubeStructure = cubeStructure;
		this.aggregationOTStates = aggregationOTStates;
	}

	public static CubeOTState create(CubeStructure cubeStructure) {
		Map<String, AggregationOTState> aggregationOTStates = new HashMap<>();
		for (Map.Entry<String, AggregationStructure> entry : cubeStructure.getAggregationStructures().entrySet()) {
			aggregationOTStates.put(entry.getKey(), new AggregationOTState(entry.getValue()));
		}
		return new CubeOTState(cubeStructure, aggregationOTStates);
	}

	@Override
	public void init() {
		for (AggregationOTState aggregationOTState : aggregationOTStates.values()) {
			aggregationOTState.init();
		}
	}

	@Override
	public void apply(CubeDiff op) {
		for (Map.Entry<String, AggregationDiff> entry : op.entrySet()) {
			aggregationOTStates.get(entry.getKey()).apply(entry.getValue());
		}
	}

	public Map<String, AggregationOTState> getAggregationStates() {
		return Collections.unmodifiableMap(aggregationOTStates);
	}

	public AggregationOTState getAggregationState(String id) {
		return aggregationOTStates.get(id);
	}

	public Map<String, Set<AggregationChunk>> getIrrelevantChunks() {
		Map<String, Set<AggregationChunk>> irrelevantChunks = new HashMap<>();
		for (Map.Entry<String, AggregationOTState> entry : aggregationOTStates.entrySet()) {
			String aggregationId = entry.getKey();
			AggregationOTState state = entry.getValue();
			AggregationStructure structure = cubeStructure.getAggregationStructure(aggregationId);
			AggregationPredicate containerPredicate = cubeStructure.getAggregationStructure(aggregationId).getPredicate();
			List<String> keys = structure.getKeys();
			for (AggregationChunk chunk : state.getChunks().values()) {
				PrimaryKey minPrimaryKey = chunk.getMinPrimaryKey();
				PrimaryKey maxPrimaryKey = chunk.getMaxPrimaryKey();
				AggregationPredicate chunkPredicate = alwaysTrue();
				for (int i = 0; i < keys.size(); i++) {
					String key = keys.get(i);
					FieldType<?> keyType = structure.getKeyType(key);
					Object minKey = keyType.toInitialValue(minPrimaryKey.get(i));
					Object maxKey = keyType.toInitialValue(maxPrimaryKey.get(i));
					if (Objects.equals(minKey, maxKey)) {
						chunkPredicate = and(chunkPredicate, eq(key, minKey));
					} else {
						chunkPredicate = and(chunkPredicate, between(key, (Comparable<?>) minKey, (Comparable<?>) maxKey));
						break;
					}
				}
				AggregationPredicate intersection = and(chunkPredicate, containerPredicate).simplify();
				if (intersection == alwaysFalse()) {
					irrelevantChunks.computeIfAbsent(aggregationId, $ -> new HashSet<>()).add(chunk);
				}
			}
		}
		return irrelevantChunks;
	}

	public boolean containsExcessiveNumberOfOverlappingChunks(int maxOverlappingChunksToProcessLogs) {
		boolean excessive = false;

		for (Map.Entry<String, AggregationOTState> entry : aggregationOTStates.entrySet()) {
			int numberOfOverlappingChunks = entry.getValue().getNumberOfOverlappingChunks();
			if (numberOfOverlappingChunks > maxOverlappingChunksToProcessLogs) {
				logger.info("Aggregation {} contains {} overlapping chunks", entry.getKey(), numberOfOverlappingChunks);
				excessive = true;
			}
		}

		return excessive;
	}

	public Set<Object> getAllChunks() {
		Set<Object> chunks = new HashSet<>();
		for (AggregationOTState state : aggregationOTStates.values()) {
			chunks.addAll(state.getChunks().keySet());
		}
		return chunks;
	}

	public Map<String, List<ConsolidationDebugInfo>> getConsolidationDebugInfo() {
		Map<String, List<ConsolidationDebugInfo>> m = new HashMap<>();
		for (Map.Entry<String, AggregationOTState> entry : aggregationOTStates.entrySet()) {
			m.put(entry.getKey(), entry.getValue().getConsolidationDebugInfo());
		}
		return m;
	}

	public List<CompatibleAggregations> findCompatibleAggregations(
		List<String> dimensions, List<String> storedMeasures, AggregationPredicate where
	) {
		Set<String> compatibleAggregations = cubeStructure.getCompatibleAggregationsForQuery(dimensions, storedMeasures, where);

		List<CompatibleAggregations> result = new ArrayList<>();

		record AggregationSortRecord(
			String key,
			AggregationStructure structure,
			AggregationOTState state,
			double score
		) implements Comparable<AggregationSortRecord> {

			@Override
			public int compareTo(AggregationSortRecord o) {
				int result;
				result = -Integer.compare(structure.getMeasures().size(), o.structure.getMeasures().size());
				if (result != 0) return result;
				result = Double.compare(score, o.score);
				if (result != 0) return result;
				result = Integer.compare(state.getChunksSize(), o.state.getChunksSize());
				if (result != 0) return result;
				result = Integer.compare(structure.getKeys().size(), o.structure.getKeys().size());
				return result;
			}
		}

		List<AggregationSortRecord> containerWithScores = new ArrayList<>();
		for (String aggregationId : compatibleAggregations) {
			AggregationStructure structure = cubeStructure.getAggregationStructure(aggregationId);
			AggregationOTState state = aggregationOTStates.get(aggregationId);
			double score = state.estimateCost(storedMeasures, where, structure);
			containerWithScores.add(new AggregationSortRecord(aggregationId, structure, state, score));
		}
		sort(containerWithScores);

		storedMeasures = new ArrayList<>(storedMeasures);
		for (AggregationSortRecord record : containerWithScores) {
			List<String> compatibleMeasures = storedMeasures.stream().filter(record.structure().getMeasures()::contains).collect(toList());
			if (compatibleMeasures.isEmpty())
				continue;
			storedMeasures.removeAll(compatibleMeasures);

			List<AggregationChunk> chunks = record.state.findChunks(
				compatibleMeasures,
				where,
				record.structure()
			);
			result.add(new CompatibleAggregations(record.key(), compatibleMeasures, chunks));
		}

		return result;
	}

	public record CompatibleAggregations(String id, List<String> measures, List<AggregationChunk> chunks) {
	}
}
