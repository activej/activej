package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.cube.aggregation.ChunkIdJsonCodec;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.linear.CubeMySqlOTUplink;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogProcessor;
import io.activej.ot.OTCommit;
import io.activej.ot.OTState;
import io.activej.ot.OTStateManager;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.reactor.Reactor;
import org.junit.function.ThrowingRunnable;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toSet;

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

	public static <T> void runProcessLogs(IAggregationChunkStorage<Long> aggregationChunkStorage, OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager, LogProcessor<T, CubeDiff> logProcessor) {
		LogDiff<CubeDiff> logDiff = await(logProcessor.processLog());
		await(aggregationChunkStorage
			.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).map(id -> (long) id).collect(toSet())));
		logCubeStateManager.add(logDiff);
		await(logCubeStateManager.sync());
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

	public static AggregationStructureBuilder aggregationStructureBuilder(ChunkIdJsonCodec<?> chunkIdCodec) {
		return new AggregationStructureBuilder(new AggregationStructure(chunkIdCodec));
	}

	public static AggregationQueryBuilder aggregationQueryBuilder() {
		return new AggregationQueryBuilder(new AggregationQuery());
	}

	public static AggregationExecutorBuilder aggregationExecutorBuilder(
		Reactor reactor, Executor executor, DefiningClassLoader classLoader,
		IAggregationChunkStorage<?> aggregationChunkStorage, FrameFormat frameFormat,
		AggregationStructure structure
	) {
		AggregationExecutor aggregationExecutor = new AggregationExecutor(reactor, executor, classLoader, aggregationChunkStorage, frameFormat, structure);
		return new AggregationExecutorBuilder(aggregationExecutor);
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

	public static final class AggregationExecutorBuilder extends AbstractBuilder<AggregationExecutorBuilder, AggregationExecutor> {
		private final AggregationExecutor executor;

		private AggregationExecutorBuilder(AggregationExecutor executor) {
			this.executor = executor;
		}

		public AggregationExecutorBuilder withTemporarySortDir(Path temporarySortDir) {
			checkNotBuilt(this);
			executor.setTemporarySortDir(temporarySortDir);
			return this;
		}

		@Override
		protected AggregationExecutor doBuild() {
			return executor;
		}
	}

	public static final class AggregationQueryBuilder extends AbstractBuilder<AggregationQueryBuilder, AggregationQuery> {
		private final AggregationQuery query;

		private AggregationQueryBuilder(AggregationQuery query) {
			this.query = query;
		}

		public AggregationQueryBuilder withKeys(String... keys) {
			checkNotBuilt(this);
			query.addKeys(List.of(keys));
			return this;
		}

		public AggregationQueryBuilder withMeasures(String... fields) {
			checkNotBuilt(this);
			query.addMeasures(List.of(fields));
			return this;
		}

		@Override
		protected AggregationQuery doBuild() {
			return query;
		}
	}
}
