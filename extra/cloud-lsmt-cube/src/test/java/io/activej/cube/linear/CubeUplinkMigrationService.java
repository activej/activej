package io.activej.cube.linear;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.collection.CollectionUtils;
import io.activej.cube.CubeStructure;
import io.activej.cube.linear.CubeMySqlOTUplink.UplinkProtoCommit;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeOT;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.eventloop.Eventloop;
import io.activej.json.JsonCodec;
import io.activej.ot.OTAlgorithms;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.cube.TestUtils.dataSource;
import static io.activej.cube.json.JsonCodecs.createCubeDiffCodec;
import static io.activej.etl.json.JsonCodecs.ofLogDiff;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class CubeUplinkMigrationService {
	private static final Logger logger = LoggerFactory.getLogger(CubeUplinkMigrationService.class);

	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	private final Eventloop eventloop = Eventloop.builder()
		.withCurrentThread()
		.withFatalErrorHandler(rethrow())
		.build();
	private final Executor executor = newSingleThreadExecutor();

	@VisibleForTesting
	CubeStructure structure = CubeStructure.builder()
		// .withAggregation(...) - CONFIGURE CUBE STRUCTURE!
		.build();

	public void migrate(DataSource repoDataSource, DataSource uplinkDataSource) throws ExecutionException, InterruptedException {
		doMigrate(repoDataSource, uplinkDataSource, null);
	}

	public void migrate(DataSource repoDataSource, DataSource uplinkDataSource, long startRevision) throws ExecutionException, InterruptedException {
		doMigrate(repoDataSource, uplinkDataSource, startRevision);
	}

	private void doMigrate(DataSource repoDataSource, DataSource uplinkDataSource, @Nullable Long startRevision) throws ExecutionException, InterruptedException {
		AsyncOTRepository<Long, LogDiff<CubeDiff>> repo = createRepo(repoDataSource);
		CubeMySqlOTUplink uplink = createUplink(uplinkDataSource);

		CompletableFuture<AsyncOTUplink.FetchData<Long, LogDiff<CubeDiff>>> future = eventloop.submit(() ->
			uplink.checkout()
				.then(checkoutData -> {
					if (checkoutData.level() != 0 ||
						checkoutData.commitId() != 0 ||
						!checkoutData.diffs().isEmpty()
					) {
						throw new IllegalStateException("Uplink repository is not empty");
					}
					//noinspection Convert2MethodRef
					return startRevision == null ?
						repo.getHeads().map(iterable -> CollectionUtils.first(iterable)) :
						Promise.of(startRevision);
				})
				.then(head -> {
					logger.info("Migrating starting from commit {}", head);
					return OTAlgorithms.checkout(repo, OT_SYSTEM, head);
				})
				.whenResult(diffs -> logger.info("Found {} diffs to be migrated", diffs.size()))
				.map(OT_SYSTEM::squash)
				.then(diffs -> uplink.push(new UplinkProtoCommit(0, diffs)))
				.whenResult(fetchData -> logger.info("Successfully migrated to uplink revision {}", fetchData.commitId()))
		);

		eventloop.run();
		future.get();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			throw new IllegalArgumentException(
				"2 program arguments required: " +
				"<path to repository data source config>, <path to uplink data source config>");
		}

		DataSource repoDataSource = dataSource(args[0]);
		DataSource uplinkDataSource = dataSource(args[1]);

		Long startRevision = args.length == 3 ? Long.parseLong(args[2]) : null;

		CubeUplinkMigrationService service = new CubeUplinkMigrationService();
		service.doMigrate(repoDataSource, uplinkDataSource, startRevision);
	}

	private AsyncOTRepository<Long, LogDiff<CubeDiff>> createRepo(DataSource dataSource) {
		JsonCodec<LogDiff<CubeDiff>> codec = ofLogDiff(createCubeDiffCodec(structure));
		AsyncSupplier<Long> idGenerator = () -> {throw new AssertionError();};
		return MySqlOTRepository.create(eventloop, executor, dataSource, idGenerator, OT_SYSTEM, codec);
	}

	private CubeMySqlOTUplink createUplink(DataSource dataSource) {
		return CubeMySqlOTUplink.create(eventloop, executor, structure, dataSource);
	}
}
