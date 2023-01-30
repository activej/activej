package io.activej.cube.linear;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdJsonCodec;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.FrameFormat_LZ4;
import io.activej.cube.Cube;
import io.activej.cube.TestUtils;
import io.activej.cube.exception.CubeException;
import io.activej.cube.linear.CubeBackupController.IChunksBackupService;
import io.activej.cube.linear.CubeMySqlOTUplink.UplinkProtoCommit;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileSystem;
import io.activej.fs.IFileSystem;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.AggregationChunkStorage.LOG;
import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.test.TestUtils.dataSource;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public class CubeBackupControllerTest {

	private static final List<String> MEASURES = List.of("pubRequests");
	private static final PrimaryKey MIN_KEY = PrimaryKey.ofArray(100);
	private static final PrimaryKey MAX_KEY = PrimaryKey.ofArray(300);
	private static final int COUNT = 12345;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Eventloop eventloop;
	private Thread eventloopThread;
	private DataSource dataSource;
	private IFileSystem fileSystem;
	private CubeMySqlOTUplink uplink;
	private CubeBackupController backupController;

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

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		FileSystem fs = FileSystem.create(eventloop, executor, aggregationsDir);
		eventloop.submit(fs::start).get();
		fileSystem = fs;
		AggregationChunkStorage<Long> aggregationChunkStorage = AggregationChunkStorage.create(eventloop, ChunkIdJsonCodec.ofLong(), AsyncSupplier.of(new RefLong(0)::inc),
				FrameFormat_LZ4.create(), fs);
		Cube cube = Cube.builder(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests", "pubRequests"))
				.build();

		IChunksBackupService chunksBackupService = IChunksBackupService.ofReactiveAggregationChunkStorage(aggregationChunkStorage);
		backupController = CubeBackupController.create(dataSource, chunksBackupService);
		uplink = CubeMySqlOTUplink.create(eventloop, executor, dataSource, PrimaryKeyCodecs.ofCube(cube));
		backupController.initialize();
		backupController.truncateTables();
	}

	@After
	public void tearDown() throws Exception {
		eventloop.keepAlive(false);
		eventloopThread.join();
	}

	@Test
	public void backup() throws CubeException {
		List<LogDiff<CubeDiff>> diffs1 = List.of(
				LogDiff.of(Map.of(
								"a", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("a", 12), 100)),
								"b", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("b", 1123), 1000))),
						CubeDiff.of(Map.of(
								"pub", AggregationDiff.of(Set.of(chunk(12), chunk(123), chunk(500))),
								"adv", AggregationDiff.of(Set.of(chunk(10), chunk(1), chunk(44)))))
				));
		await(() -> uplink.push(new UplinkProtoCommit(0, diffs1)));
		uploadStubChunks(diffs1);

		List<LogDiff<CubeDiff>> diffs2 = List.of(
				LogDiff.of(Map.of(
								"b", new LogPositionDiff(LogPosition.create(new LogFile("b", 1123), 1000), LogPosition.create(new LogFile("b2", 9), 2341)),
								"c", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("c", 555), 12))),
						CubeDiff.of(Map.of(
								"pub", AggregationDiff.of(Set.of(chunk(43)), Set.of(chunk(500))),
								"adv", AggregationDiff.of(Set.of(chunk(512), chunk(786)), Set.of(chunk(44)))))
				));
		await(() -> uplink.push(new UplinkProtoCommit(1, diffs2)));
		uploadStubChunks(diffs2);

		List<LogDiff<CubeDiff>> diffs3 = List.of(
				LogDiff.of(Map.of(
								"d", new LogPositionDiff(LogPosition.initial(), LogPosition.create(new LogFile("d", 541), 5235))),
						CubeDiff.of(Map.of(
								"pub", AggregationDiff.of(Set.of(chunk(4566)), Set.of(chunk(12)))))
				));
		await(() -> uplink.push(new UplinkProtoCommit(2, diffs3)));
		uploadStubChunks(diffs3);

		backupController.backup(0);
		backupController.backup(1);
		backupController.backup(2);
		backupController.backup(3);

		assertBackups(0, 1, 2, 3);

		assertChunkIds(0, Set.of());
		assertPositions(0, Map.of());

		assertChunkIds(1, Set.of(1L, 10L, 12L, 44L, 123L, 500L));
		assertPositions(1, Map.of(
				"a", LogPosition.create(new LogFile("a", 12), 100),
				"b", LogPosition.create(new LogFile("b", 1123), 1000)));

		assertChunkIds(2, Set.of(1L, 10L, 12L, 43L, 123L, 512L, 786L));
		assertPositions(2, Map.of(
				"a", LogPosition.create(new LogFile("a", 12), 100),
				"b", LogPosition.create(new LogFile("b2", 9), 2341),
				"c", LogPosition.create(new LogFile("c", 555), 12)));

		assertChunkIds(3, Set.of(1L, 10L, 43L, 123L, 512L, 786L, 4566L));
		assertPositions(3, Map.of(
				"a", LogPosition.create(new LogFile("a", 12), 100),
				"b", LogPosition.create(new LogFile("b2", 9), 2341),
				"c", LogPosition.create(new LogFile("c", 555), 12),
				"d", LogPosition.create(new LogFile("d", 541), 5235)));
	}

	private static AggregationChunk chunk(long id) {
		return AggregationChunk.create(id, MEASURES, MIN_KEY, MAX_KEY, COUNT);
	}

	private <T> T await(AsyncSupplier<T> supplier) {
		return TestUtils.asyncAwait(eventloop, supplier);
	}

	private void uploadStubChunks(List<LogDiff<CubeDiff>> diffs) {
		await(() -> Promises.all(diffs.stream()
				.map(LogDiff::getDiffs)
				.flatMap(Collection::stream)
				.flatMap(CubeDiff::addedChunks)
				.map(String::valueOf)
				.map(name -> fileSystem.upload(name + LOG)
						.then(consumer -> consumer.acceptAll(wrapUtf8("Stub chunk data"), null)))));
	}

	private void assertBackups(long... backupIds) {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement stmt = connection.prepareStatement("" +
					"SELECT `revision` " +
					"FROM " + CubeBackupController.BACKUP_REVISION_TABLE
			)) {
				ResultSet resultSet = stmt.executeQuery();

				Set<Long> ids = new HashSet<>();
				while (resultSet.next()) {
					ids.add(resultSet.getLong(1));
				}

				assertEquals(Arrays.stream(backupIds).boxed().collect(toSet()), ids);
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}

	private void assertChunkIds(long backupId, Set<Long> chunkIds) {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement stmt = connection.prepareStatement("" +
					"SELECT `id`, `added_revision` <= `backup_id` " +
					"FROM " + CubeBackupController.BACKUP_CHUNK_TABLE +
					" WHERE `backup_id` = ? "
			)) {
				stmt.setLong(1, backupId);

				ResultSet resultSet = stmt.executeQuery();

				Set<Long> ids = new HashSet<>();
				while (resultSet.next()) {
					assertTrue(resultSet.getBoolean(2));

					ids.add(resultSet.getLong(1));
				}

				assertEquals(chunkIds, ids);
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}

		String prefix = "backups" + IFileSystem.SEPARATOR + backupId + IFileSystem.SEPARATOR;
		Set<Long> actualChunks = await(() -> fileSystem.list(prefix + "*" + LOG))
				.keySet()
				.stream()
				.map(s -> s.substring(prefix.length(), s.length() - LOG.length()))
				.map(Long::parseLong)
				.collect(toSet());

		assertEquals(actualChunks, chunkIds);
	}

	private void assertPositions(long backupId, Map<String, LogPosition> positions) {
		try (Connection connection = dataSource.getConnection()) {
			try (PreparedStatement stmt = connection.prepareStatement("" +
					"SELECT `partition_id`, `filename`, `remainder`, `position` " +
					"FROM " + CubeBackupController.BACKUP_POSITION_TABLE +
					" WHERE `backup_id` = ? "
			)) {
				stmt.setLong(1, backupId);

				ResultSet resultSet = stmt.executeQuery();

				Map<String, LogPosition> positionMap = new HashMap<>();
				while (resultSet.next()) {
					String partitionId = resultSet.getString(1);
					String filename = resultSet.getString(2);
					int remainder = resultSet.getInt(3);
					long position = resultSet.getLong(4);

					LogFile logFile = new LogFile(filename, remainder);
					LogPosition logPosition = LogPosition.create(logFile, position);
					assertNull(positionMap.put(partitionId, logPosition));
				}

				assertEquals(positions, positionMap);
			}
		} catch (SQLException e) {
			throw new AssertionError(e);
		}
	}

}
