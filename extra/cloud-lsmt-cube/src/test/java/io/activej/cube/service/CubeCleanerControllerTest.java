package io.activej.cube.service;

import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.Cube;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffCodec;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.cube.ot.CubeOT;
import io.activej.etl.LogDiff;
import io.activej.etl.LogDiffCodec;
import io.activej.etl.LogOT;
import io.activej.fs.LocalFs;
import io.activej.ot.OTCommit;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;

public class CubeCleanerControllerTest {
	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Reactor reactor;
	private MySqlOTRepository<LogDiff<CubeDiff>> repository;
	private AggregationChunkStorage<Long> aggregationChunkStorage;

	@Before
	public void setUp() throws Exception {
		DataSource dataSource = dataSource("test.properties");
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		reactor = Reactor.getCurrentReactor();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		aggregationChunkStorage = AggregationChunkStorage.create(reactor, ChunkIdCodec.ofLong(), AsyncSupplier.of(new RefLong(0)::inc),
				LZ4FrameFormat.create(), LocalFs.create(reactor, executor, aggregationsDir));
		Cube cube = Cube.create(reactor, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		repository = MySqlOTRepository.create(reactor, executor, dataSource, AsyncSupplier.of(new RefLong(0)::inc),
				OT_SYSTEM, LogDiffCodec.create(CubeDiffCodec.create(cube)));
		repository.initialize();
		repository.truncateTables();
	}

	@Test
	public void testCleanupWithExtraSnapshotsCount() throws IOException, SQLException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(reactor,
						CubeDiffScheme.ofLogDiffs(), repository, OT_SYSTEM, aggregationChunkStorage)
				.withFreezeTimeout(Duration.ofMillis(0))
				.withExtraSnapshotsCount(1000);

		await(cleanerController.cleanup());
	}

	@Test
	public void testCleanupWithFreezeTimeout() throws IOException, SQLException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(reactor,
						CubeDiffScheme.ofLogDiffs(), repository, OT_SYSTEM, aggregationChunkStorage)
				.withFreezeTimeout(Duration.ofSeconds(10));

		await(cleanerController.cleanup());
	}

	public void initializeRepo() throws IOException, SQLException {
		repository.initialize();
		repository.truncateTables();

		Long id1 = await(repository.createCommitId());
		await(repository.push(OTCommit.ofRoot(id1)));                          // 1N

		Long id2 = await(repository.createCommitId());
		await(repository.push(OTCommit.ofCommit(0, id2, id1, List.of(), id1))); // 2N

		Long id3 = await(repository.createCommitId());
		await(repository.push(OTCommit.ofCommit(0, id3, id2, List.of(), id2))); // 3N

		Long id4 = await(repository.createCommitId());
		await(repository.push(OTCommit.ofCommit(0, id4, id3, List.of(), id3)));
		await(repository.saveSnapshot(id4, List.of()));                      // 4S

		Long id5 = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(OTCommit.ofCommit(0, id5, id4, List.of(), id4))); // 5N
	}

}
