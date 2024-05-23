package io.activej.cube;

import io.activej.cube.AggregationState.ConsolidationDebugInfo;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.ot.CubeDiff;
import io.activej.ot.OTState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.activej.cube.aggregation.predicate.AggregationPredicates.alwaysFalse;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.and;
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
			//noinspection rawtypes
			Map<String, FieldType> keyTypes = structure.getKeyTypes();
			for (AggregationChunk chunk : state.getChunks().values()) {
				AggregationPredicate chunkPredicate = chunk.toPredicate(keys, keyTypes);
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
			AggregationState state,
			double score
		) implements Comparable<AggregationSortRecord> {

			@Override
			public int compareTo(AggregationSortRecord o) {
				int result;
				result = -Integer.compare(state.getStructure().getMeasures().size(), o.state.getStructure().getMeasures().size());
				if (result != 0) return result;
				result = Double.compare(score, o.score);
				if (result != 0) return result;
				result = Integer.compare(state.getChunksSize(), o.state.getChunksSize());
				if (result != 0) return result;
				result = Integer.compare(state.getStructure().getKeys().size(), o.state.getStructure().getKeys().size());
				return result;
			}
		}

		List<AggregationSortRecord> containerWithScores = new ArrayList<>();
		for (String aggregationId : compatibleAggregations) {
			AggregationState state = aggregationStates.get(aggregationId);
			double score = state.estimateCost(storedMeasures, where);
			containerWithScores.add(new AggregationSortRecord(aggregationId, state, score));
		}
		sort(containerWithScores);

		storedMeasures = new ArrayList<>(storedMeasures);
		for (AggregationSortRecord record : containerWithScores) {
			List<String> aggregationMeasures = record.state().getStructure().getMeasures();
			List<String> compatibleMeasures = storedMeasures.stream().filter(aggregationMeasures::contains).collect(toList());
			if (compatibleMeasures.isEmpty())
				continue;
			storedMeasures.removeAll(compatibleMeasures);

			List<AggregationChunk> chunks = record.state.findChunks(
				compatibleMeasures,
				where
			);
			if (chunks.isEmpty()) continue;
			result.add(new CompatibleAggregations(record.key(), compatibleMeasures, chunks));
		}

		return result;
	}

	public record CompatibleAggregations(String id, List<String> measures, List<AggregationChunk> chunks) {
	}
}
