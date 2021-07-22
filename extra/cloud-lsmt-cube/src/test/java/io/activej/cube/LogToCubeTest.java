package io.activej.cube;

import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public final class LogToCubeTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

//	@Test
//	public void testStubStorage() throws Exception {
//		Path aggregationsDir = temporaryFolder.newFolder().toPath();
//		Path logsDir = temporaryFolder.newFolder().toPath();
//
//		Eventloop eventloop = Eventloop.getCurrentEventloop();
//		Executor executor = Executors.newCachedThreadPool();
//		DefiningClassLoader classLoader = DefiningClassLoader.create();
//
//		LocalActiveFs fs = LocalActiveFs.create(eventloop, executor, aggregationsDir);
//		await(fs.start());
//		FrameFormat frameFormat = LZ4FrameFormat.create();
//		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), frameFormat, fs);
//		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("pub", ofInt())
//				.withDimension("adv", ofInt())
//				.withMeasure("pubRequests", sum(ofLong()))
//				.withMeasure("advRequests", sum(ofLong()))
//				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
//				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));
//
//		DataSource dataSource = dataSource("test.properties");
//		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
//		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//				otSystem, LogDiffCodec.create(CubeDiffCodec.create(cube)));
//		initializeRepository(repository);
//
//		List<TestAdvResult> expected = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));
//
//		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
//		OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> node = OTUplinkImpl.create(repository, otSystem);
//		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, otSystem, node, cubeDiffLogOTState);
//
//		LocalActiveFs localFs = LocalActiveFs.create(eventloop, executor, logsDir);
//		await(localFs.start());
//		Multilog<TestPubRequest> multilog = MultilogImpl.create(eventloop, localFs,
//				frameFormat,
//				SerializerBuilder.create(classLoader).build(TestPubRequest.class),
//				NAME_PARTITION_REMAINDER_SEQ);
//
//		LogOTProcessor<TestPubRequest, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
//				multilog,
//				new TestAggregatorSplitter(cube), // TestAggregatorSplitter.create(eventloop, cube),
//				"testlog",
//				asList("partitionA"),
//				cubeDiffLogOTState);
//
//		StreamSupplier<TestPubRequest> supplier = StreamSupplier.of(
//				new TestPubRequest(1000, 1, asList(new TestAdvRequest(10))),
//				new TestPubRequest(1001, 2, asList(new TestAdvRequest(10), new TestAdvRequest(20))),
//				new TestPubRequest(1002, 1, asList(new TestAdvRequest(30))),
//				new TestPubRequest(1002, 2, Arrays.asList()));
//
//		await(supplier.streamTo(StreamConsumer.ofPromise(multilog.write("partitionA"))));
//		await(logCubeStateManager.checkout());
//		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);
//
//		List<TestAdvResult> list = await(cube.queryRawStream(
//				asList("adv"),
//				asList("advRequests"),
//				alwaysTrue(),
//				TestAdvResult.class, classLoader)
//				.toList());
//
//		assertEquals(expected, list);
//	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

		@SuppressWarnings("unused")
		public TestAdvResult() {
		}

		public TestAdvResult(int adv, long advRequests) {
			this.adv = adv;
			this.advRequests = advRequests;
		}

		@Override
		public String toString() {
			return "TestAdvResult{adv=" + adv + ", advRequests=" + advRequests + '}';
		}

		@SuppressWarnings("RedundantIfStatement")
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestAdvResult that = (TestAdvResult) o;

			if (adv != that.adv) return false;
			if (advRequests != that.advRequests) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = adv;
			result = 31 * result + (int) (advRequests ^ (advRequests >>> 32));
			return result;
		}
	}
}
