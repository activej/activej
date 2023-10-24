package io.activej.cube;

import io.activej.cube.AggregationState.ConsolidationDebugInfo;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.ot.CubeDiff;
import io.activej.ot.OTState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.activej.cube.aggregation.predicate.AggregationPredicates.*;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toList;

public final class CubeState implements OTState<CubeDiff> {
	private static final Logger logger = LoggerFactory.getLogger(CubeState.class);

	private final CubeStructure cubeStructure;
	private final Map<String, AggregationState> aggregationStates;

	private CubeState(CubeStructure cubeStructure, Map<String, AggregationState> aggregationStates) {
		this.cubeStructure = cubeStructure;
		this.aggregationStates = aggregationStates;
	}

	public static CubeState create(CubeStructure cubeStructure) {
		Map<String, AggregationState> aggregationStates = new HashMap<>();
		for (Map.Entry<String, AggregationStructure> entry : cubeStructure.getAggregationStructures().entrySet()) {
			aggregationStates.put(entry.getKey(), new AggregationState(entry.getValue()));
		}
		return new CubeState(cubeStructure, aggregationStates);
	}

	@Override
	public void init() {
		for (AggregationState aggregationState : aggregationStates.values()) {
			aggregationState.init();
		}
	}

	@Override
	public void apply(CubeDiff op) {
		for (Map.Entry<String, AggregationDiff> entry : op.entrySet()) {
			aggregationStates.get(entry.getKey()).apply(entry.getValue());
		}
	}

	public Map<String, AggregationState> getAggregationStates() {
		return Collections.unmodifiableMap(aggregationStates);
	}

	public AggregationState getAggregationState(String id) {
		return aggregationStates.get(id);
	}

	public Map<String, Set<AggregationChunk>> getIrrelevantChunks() {
		Map<String, Set<AggregationChunk>> irrelevantChunks = new HashMap<>();
		for (Map.Entry<String, AggregationState> entry : aggregationStates.entrySet()) {
			String aggregationId = entry.getKey();
			AggregationState state = entry.getValue();
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

		for (Map.Entry<String, AggregationState> entry : aggregationStates.entrySet()) {
			int numberOfOverlappingChunks = entry.getValue().getNumberOfOverlappingChunks();
			if (numberOfOverlappingChunks > maxOverlappingChunksToProcessLogs) {
				logger.info("Aggregation {} contains {} overlapping chunks", entry.getKey(), numberOfOverlappingChunks);
				excessive = true;
			}
		}

		return excessive;
	}

	public Set<Long> getAllChunks() {
		Set<Long> chunks = new HashSet<>();
		for (AggregationState state : aggregationStates.values()) {
			chunks.addAll(state.getChunks().keySet());
		}
		return chunks;
	}

	public Map<String, List<ConsolidationDebugInfo>> getConsolidationDebugInfo() {
		Map<String, List<ConsolidationDebugInfo>> m = new HashMap<>();
		for (Map.Entry<String, AggregationState> entry : aggregationStates.entrySet()) {
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
			AggregationState state,
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
			AggregationState state = aggregationStates.get(aggregationId);
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
