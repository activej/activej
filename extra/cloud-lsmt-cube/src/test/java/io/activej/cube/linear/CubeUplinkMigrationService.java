package io.activej.cube.linear;

import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.Utils;
import io.activej.cube.Cube;
import io.activej.cube.linear.CubeUplinkMySql.UplinkProtoCommit;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffCodec;
import io.activej.cube.ot.CubeOT;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.LogDiff;
import io.activej.etl.LogDiffCodec;
import io.activej.etl.LogOT;
import io.activej.eventloop.Eventloop;
import io.activej.ot.OTAlgorithms;
import io.activej.ot.repository.OTRepositoryMySql;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplink;
import io.activej.ot.util.IdGenerator;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.test.TestUtils.dataSource;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

final class CubeUplinkMigrationService {
	private static final Logger logger = LoggerFactory.getLogger(CubeUplinkMigrationService.class);

	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	private final Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
	private final Executor executor = newSingleThreadExecutor();

	@VisibleForTesting
	Cube cube = createEmptyCube(eventloop, executor)
			// .withAggregation(...) - CONFIGURE CUBE STRUCTURE!
			;

	public void migrate(DataSource repoDataSource, DataSource uplinkDataSource) throws ExecutionException, InterruptedException {
		doMigrate(repoDataSource, uplinkDataSource, null);
	}

	public void migrate(DataSource repoDataSource, DataSource uplinkDataSource, long startRevision) throws ExecutionException, InterruptedException {
		doMigrate(repoDataSource, uplinkDataSource, startRevision);
	}

	private void doMigrate(DataSource repoDataSource, DataSource uplinkDataSource, @Nullable Long startRevision) throws ExecutionException, InterruptedException {
		OTRepositoryMySql<LogDiff<CubeDiff>> repo = createRepo(repoDataSource);
		CubeUplinkMySql uplink = createUplink(uplinkDataSource);

		CompletableFuture<OTUplink.FetchData<Long, LogDiff<CubeDiff>>> future = eventloop.submit(() ->
				uplink.checkout()
						.then(checkoutData -> {
							if (checkoutData.getLevel() != 0 ||
									checkoutData.getCommitId() != 0 ||
									!checkoutData.getDiffs().isEmpty()) {
								throw new IllegalStateException("Uplink repository is not empty");
							}
							//noinspection Convert2MethodRef
							return startRevision == null ?
									repo.getHeads().map(iterable -> Utils.first(iterable)) :
									Promise.of(startRevision);
						})
						.then(head -> {
							logger.info("Migrating starting from commit {}", head);
							return OTAlgorithms.checkout(repo, OT_SYSTEM, head);
						})
						.whenResult(diffs -> logger.info("Found {} diffs to be migrated", diffs.size()))
						.map(OT_SYSTEM::squash)
						.then(diffs -> uplink.push(new UplinkProtoCommit(0, diffs)))
						.whenResult(fetchData -> logger.info("Successfully migrated to uplink revision {}", fetchData.getCommitId()))
		);

		eventloop.run();
		future.get();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			throw new IllegalArgumentException("2 program arguments required: " +
					"<path to repository data source config>, <path to uplink data source config>");
		}

		DataSource repoDataSource = dataSource(args[0]);
		DataSource uplinkDataSource = dataSource(args[1]);

		Long startRevision = args.length == 3 ? Long.parseLong(args[2]) : null;

		CubeUplinkMigrationService service = new CubeUplinkMigrationService();
		service.doMigrate(repoDataSource, uplinkDataSource, startRevision);
	}

	private OTRepositoryMySql<LogDiff<CubeDiff>> createRepo(DataSource dataSource) {
		LogDiffCodec<CubeDiff> codec = LogDiffCodec.create(CubeDiffCodec.create(cube));
		IdGenerator<Long> idGenerator = () -> {
			throw new AssertionError();
		};
		return OTRepositoryMySql.create(eventloop, executor, dataSource, idGenerator, OT_SYSTEM, codec);
	}

	private CubeUplinkMySql createUplink(DataSource dataSource) {
		return CubeUplinkMySql.create(executor, dataSource, PrimaryKeyCodecs.ofCube(cube));
	}

	static Cube createEmptyCube(Eventloop eventloop, Executor executor) {
		return Cube.create(eventloop, executor, DefiningClassLoader.create(), new AggregationChunkStorage<Long>() {
			@Override
			public Promise<Long> createId() {
				throw new AssertionError();
			}

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				throw new AssertionError();
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				throw new AssertionError();
			}

			@Override
			public Promise<Void> finish(Set<Long> chunkIds) {
				throw new AssertionError();
			}
		});
	}
}
