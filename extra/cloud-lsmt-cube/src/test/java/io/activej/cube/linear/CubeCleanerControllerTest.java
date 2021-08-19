package io.activej.cube.linear;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.Cube;
import io.activej.cube.IdGeneratorStub;
import io.activej.cube.TestUtils;
import io.activej.cube.exception.CubeException;
import io.activej.cube.linear.CubeCleanerController.ChunksCleanerService;
import io.activej.cube.linear.CubeUplinkMySql.UplinkProtoCommit;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.test.rules.ByteBufRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.cube.TestUtils.initializeUplink;
import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.test.TestUtils.dataSource;
import static java.util.Collections.emptyList;

public class CubeCleanerControllerTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Eventloop eventloop;
	private DataSource dataSource;
	private CubeUplinkMySql uplink;
	private ActiveFsChunkStorage<Long> aggregationChunkStorage;

	@Before
	public void setUp() throws Exception {
		dataSource = dataSource("test.properties");
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		eventloop = Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());

		eventloop.keepAlive(true);

		Thread thread = new Thread(eventloop);
		thread.start();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(),
				LZ4FrameFormat.create(), LocalActiveFs.create(eventloop, executor, aggregationsDir));
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		uplink = CubeUplinkMySql.create(executor, dataSource, PrimaryKeyCodecs.ofCube(cube));
		uplink.initialize();
		uplink.truncateTables();
	}

	@Test
	public void testCleanupWithExtraSnapshotsCount() throws CubeException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		ChunksCleanerService cleanerService = ChunksCleanerService.ofActiveFsChunkStorage(aggregationChunkStorage);
		CubeCleanerController cleanerController = CubeCleanerController.create(dataSource, cleanerService)
				.withChunksCleanupDelay(Duration.ofMillis(0));

		cleanerController.cleanup();
	}

	@Test
	public void testCleanupWithFreezeTimeout() throws CubeException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		ChunksCleanerService cleanerService = ChunksCleanerService.ofActiveFsChunkStorage(aggregationChunkStorage);
		CubeCleanerController cleanerController = CubeCleanerController.create(dataSource, cleanerService)
				.withChunksCleanupDelay(Duration.ofSeconds(10));

		cleanerController.cleanup();
	}

	public void initializeRepo() {
		initializeUplink(uplink);

		UplinkProtoCommit proto1 = await(() -> uplink.createProtoCommit(0L, emptyList(), 0));
		await(() -> uplink.push(proto1)); // 1N

		UplinkProtoCommit proto2 = await(() -> uplink.createProtoCommit(1L, emptyList(), 1));
		await(() -> uplink.push(proto2)); // 2N

		UplinkProtoCommit proto3 = await(() -> uplink.createProtoCommit(2L, emptyList(), 2));
		await(() -> uplink.push(proto3)); // 3N

		UplinkProtoCommit proto4 = await(() -> uplink.createProtoCommit(3L, emptyList(), 3));
		await(() -> uplink.push(proto4)); // 4S

		UplinkProtoCommit proto5 = await(() -> uplink.createProtoCommit(4L, emptyList(), 4));
		await(() -> uplink.push(proto5)); // 5N
	}

	private <T> T await(AsyncSupplier<T> supplier) {
		return TestUtils.asyncAwait(eventloop, supplier);
	}
}
