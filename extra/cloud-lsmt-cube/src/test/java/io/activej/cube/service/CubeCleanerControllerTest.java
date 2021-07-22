package io.activej.cube.service;

public class CubeCleanerControllerTest {
//	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());
//
//	@Rule
//	public final TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//	@ClassRule
//	public static final EventloopRule eventloopRule = new EventloopRule();
//
//	@ClassRule
//	public static final ByteBufRule byteBufRule = new ByteBufRule();
//
//	private Eventloop eventloop;
//	private OTRepositoryMySql<LogDiff<CubeDiff>> repository;
//	private AggregationChunkStorage<Long> aggregationChunkStorage;
//
//	@Before
//	public void setUp() throws Exception {
//		DataSource dataSource = dataSource("test.properties");
//		Path aggregationsDir = temporaryFolder.newFolder().toPath();
//		Executor executor = Executors.newCachedThreadPool();
//
//		eventloop = Eventloop.getCurrentEventloop();
//
//		DefiningClassLoader classLoader = DefiningClassLoader.create();
//		aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(),
//				LZ4FrameFormat.create(), LocalActiveFs.create(eventloop, executor, aggregationsDir));
//		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("pub", ofInt())
//				.withDimension("adv", ofInt())
//				.withMeasure("pubRequests", sum(ofLong()))
//				.withMeasure("advRequests", sum(ofLong()))
//				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
//				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));
//
//		repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//				OT_SYSTEM, LogDiffCodec.create(CubeDiffCodec.create(cube)));
//		repository.initialize();
//		repository.truncateTables();
//	}
//
//	@Test
//	public void testCleanupWithExtraSnapshotsCount() throws IOException, SQLException {
//		// 1S -> 2N -> 3N -> 4S -> 5N
//		initializeRepo();
//
//		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(eventloop,
//				CubeDiffScheme.ofLogDiffs(), repository, OT_SYSTEM, (ActiveFsChunkStorage<Long>) aggregationChunkStorage)
//				.withFreezeTimeout(Duration.ofMillis(0))
//				.withExtraSnapshotsCount(1000);
//
//		await(cleanerController.cleanup());
//	}
//
//	@Test
//	public void testCleanupWithFreezeTimeout() throws IOException, SQLException {
//		// 1S -> 2N -> 3N -> 4S -> 5N
//		initializeRepo();
//
//		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(eventloop,
//				CubeDiffScheme.ofLogDiffs(), repository, OT_SYSTEM, (ActiveFsChunkStorage<Long>) aggregationChunkStorage)
//				.withFreezeTimeout(Duration.ofSeconds(10));
//
//		await(cleanerController.cleanup());
//	}
//
//	public void initializeRepo() throws IOException, SQLException {
//		repository.initialize();
//		repository.truncateTables();
//
//		Long id1 = await(repository.createCommitId());
//		await(repository.push(OTCommit.ofRoot(id1)));                          // 1N
//
//		Long id2 = await(repository.createCommitId());
//		await(repository.push(OTCommit.ofCommit(0, id2, id1, emptyList(), id1))); // 2N
//
//		Long id3 = await(repository.createCommitId());
//		await(repository.push(OTCommit.ofCommit(0, id3, id2, emptyList(), id2))); // 3N
//
//		Long id4 = await(repository.createCommitId());
//		await(repository.push(OTCommit.ofCommit(0, id4, id3, emptyList(), id3)));
//		await(repository.saveSnapshot(id4, emptyList()));                      // 4S
//
//		Long id5 = await(repository.createCommitId());
//		await(repository.pushAndUpdateHead(OTCommit.ofCommit(0, id5, id4, emptyList(), id4))); // 5N
//	}

}
