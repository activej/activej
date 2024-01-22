package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.builder.AbstractBuilder;
import io.activej.cube.aggregation.ChunkIdGenerator;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.util.Utils;
import io.activej.cube.linear.CubeMySqlOTUplink;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogProcessor;
import io.activej.etl.LogState;
import io.activej.ot.OTCommit;
import io.activej.ot.OTState;
import io.activej.ot.StateManager;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.junit.function.ThrowingRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static io.activej.promise.TestUtils.await;

public final class TestUtils {

	public static void initializeUplink(CubeMySqlOTUplink uplink) {
		noFail(() -> {
			uplink.initialize();
			uplink.truncateTables();
		});
	}

	public static void initializeRepository(MySqlOTRepository<LogDiff<CubeDiff>> repository) {
		noFail(() -> {
			repository.initialize();
			repository.truncateTables();
		});
		Long id = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(OTCommit.ofRoot(id)));
		await(repository.saveSnapshot(id, List.of()));
	}

	public static <T> void runProcessLogs(IAggregationChunkStorage aggregationChunkStorage, StateManager<LogDiff<CubeDiff>, ?> stateManager, LogProcessor<T, ProtoCubeDiff, CubeDiff> logProcessor) {
		LogDiff<ProtoCubeDiff> logDiff = await(logProcessor.processLog());
		List<String> protoChunkIds = logDiff.diffs().flatMap(ProtoCubeDiff::addedProtoChunks).toList();
		List<Long> chunkIds = await(aggregationChunkStorage
			.finish(protoChunkIds));
		await(stateManager.push(List.of(Utils.materializeProtoCubeDiff(logDiff, protoChunkIds, chunkIds))));
	}

	public static final OTState<CubeDiff> STUB_CUBE_STATE = new OTState<>() {
		@Override
		public void init() {
		}

		@Override
		public void apply(CubeDiff op) {
		}
	};

	public static <T> T asyncAwait(Reactor reactor, AsyncSupplier<T> supplier) {
		try {
			return reactor.submit(supplier::get).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		} catch (ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	public static void noFail(ThrowingRunnable runnable) {
		try {
			runnable.run();
		} catch (Throwable e) {
			throw new AssertionError(e);
		}
	}

	public static AggregationState createAggregationState(AggregationStructure aggregationStructure) {
		return new AggregationState(aggregationStructure);
	}

	public static AggregationStructureBuilder aggregationStructureBuilder() {
		return new AggregationStructureBuilder(new AggregationStructure());
	}

	@SuppressWarnings("rawtypes")
	public static final class AggregationStructureBuilder extends AbstractBuilder<AggregationStructureBuilder, AggregationStructure> {
		private final AggregationStructure structure;

		private AggregationStructureBuilder(AggregationStructure structure) {
			this.structure = structure;
		}

		public AggregationStructureBuilder withKey(String keyId, FieldType type) {
			checkNotBuilt(this);
			structure.addKey(keyId, type);
			return this;
		}

		public AggregationStructureBuilder withMeasure(String measureId, Measure aggregateFunction) {
			checkNotBuilt(this);
			structure.addMeasure(measureId, aggregateFunction);
			return this;
		}

		@Override
		protected AggregationStructure doBuild() {
			return structure;
		}
	}

	public static StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stubStateManager(CubeStructure cubeStructure) {
		return new StateManager<>() {
			final LogState<CubeDiff, CubeState> state = LogState.create(CubeState.create(cubeStructure));

			@Override
			public Promise<Void> catchUp() {
				return Promise.complete();
			}

			@Override
			public Promise<Void> push(List<LogDiff<CubeDiff>> diffs) {
				for (LogDiff<CubeDiff> diff : diffs) {
					state.apply(diff);
				}
				return Promise.complete();
			}

			@Override
			public StateChangesSupplier<LogDiff<CubeDiff>> subscribeToStateChanges() {
				throw new UnsupportedOperationException();
			}

			@Override
			public <R> R query(Function<LogState<CubeDiff, CubeState>, R> queryFn) {
				return queryFn.apply(state);
			}
		};
	}

	public static ChunkIdGenerator stubChunkIdGenerator() {
		return new ChunkIdGenerator() {
			private long id = 0;

			@Override
			public Promise<String> createProtoChunkId() {
				return Promise.of(UUID.randomUUID().toString());
			}

			@Override
			public Promise<List<Long>> convertToActualChunkIds(List<String> protoChunkIds) {
				List<Long> result = new ArrayList<>(protoChunkIds.size());
				for (String ignored : protoChunkIds) {
					result.add(++id);
				}
				return Promise.of(result);
			}
		};
	}
}
