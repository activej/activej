package io.activej.cube.linear;

import io.activej.async.function.AsyncSupplier;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.CubeStructure;
import io.activej.cube.TestUtils;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.exception.CubeException;
import io.activej.cube.linear.CubeCleanerController.IChunksCleanerService;
import io.activej.cube.linear.CubeMySqlOTUplink.UplinkProtoCommit;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileSystem;
import io.activej.test.rules.ByteBufRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.*;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;

public class CubeCleanerControllerTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Eventloop eventloop;
	private Thread eventloopThread;
	private DataSource dataSource;
	private CubeMySqlOTUplink uplink;
	private AggregationChunkStorage aggregationChunkStorage;

	@Before
	public void setUp() throws Exception {
		dataSource = dataSource("test.properties");
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		eventloop = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.build();

		eventloop.keepAlive(true);

		eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		FileSystem fs = FileSystem.create(eventloop, executor, aggregationsDir);
		await(fs::start);
		aggregationChunkStorage = AggregationChunkStorage.create(eventloop, stubChunkIdGenerator(),
			FrameFormats.lz4(), fs);
		CubeStructure structure = CubeStructure.builder()
			.withDimension("pub", ofInt())
			.withDimension("adv", ofInt())
			.withMeasure("pubRequests", sum(ofLong()))
			.withMeasure("advRequests", sum(ofLong()))
			.withAggregation(id("pub")
				.withDimensions("pub")
				.withMeasures("pubRequests"))
			.withAggregation(id("adv")
				.withDimensions("adv")
				.withMeasures("advRequests"))
			.build();

		uplink = CubeMySqlOTUplink.create(eventloop, executor, structure, dataSource);
		uplink.initialize();
		uplink.truncateTables();
	}

	@After
	public void tearDown() throws Exception {
		eventloop.keepAlive(false);
		eventloopThread.join();
	}

	@Test
	public void testCleanupWithExtraSnapshotsCount() throws CubeException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		IChunksCleanerService cleanerService = IChunksCleanerService.ofReactiveAggregationChunkStorage(aggregationChunkStorage);
		CubeCleanerController cleanerController = CubeCleanerController.builder(dataSource, cleanerService)
			.withChunksCleanupDelay(Duration.ofMillis(0))
			.build();

		cleanerController.cleanup();
	}

	@Test
	public void testCleanupWithFreezeTimeout() throws CubeException {
		// 1S -> 2N -> 3N -> 4S -> 5N
		initializeRepo();

		IChunksCleanerService cleanerService = IChunksCleanerService.ofReactiveAggregationChunkStorage(aggregationChunkStorage);
		CubeCleanerController cleanerController = CubeCleanerController.builder(dataSource, cleanerService)
			.withChunksCleanupDelay(Duration.ofSeconds(10))
			.build();

		cleanerController.cleanup();
	}

	public void initializeRepo() {
		initializeUplink(uplink);

		UplinkProtoCommit proto1 = await(() -> uplink.createProtoCommit(0L, List.of(), 0));
		await(() -> uplink.push(proto1)); // 1N

		UplinkProtoCommit proto2 = await(() -> uplink.createProtoCommit(1L, List.of(), 1));
		await(() -> uplink.push(proto2)); // 2N

		UplinkProtoCommit proto3 = await(() -> uplink.createProtoCommit(2L, List.of(), 2));
		await(() -> uplink.push(proto3)); // 3N

		UplinkProtoCommit proto4 = await(() -> uplink.createProtoCommit(3L, List.of(), 3));
		await(() -> uplink.push(proto4)); // 4S

		UplinkProtoCommit proto5 = await(() -> uplink.createProtoCommit(4L, List.of(), 4));
		await(() -> uplink.push(proto5)); // 5N
	}

	private <T> T await(AsyncSupplier<T> supplier) {
		return TestUtils.asyncAwait(eventloop, supplier);
	}
}
