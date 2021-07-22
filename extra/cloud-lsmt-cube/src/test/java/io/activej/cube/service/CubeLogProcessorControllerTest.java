package io.activej.cube.service;

public final class CubeLogProcessorControllerTest {
//	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());
//
//	@ClassRule
//	public static final EventloopRule eventloopRule = new EventloopRule();
//
//	@ClassRule
//	public static final ByteBufRule byteBufRule = new ByteBufRule();
//
//	@Rule
//	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();
//
//	@Rule
//	public final TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//	private Multilog<LogItem> multilog;
//	private LocalActiveFs logsFs;
//	private CubeLogProcessorController<Long, Long> controller;
//
//	@Before
//	public void setUp() throws Exception {
//		DataSource dataSource = dataSource("test.properties");
//		Path aggregationsDir = temporaryFolder.newFolder().toPath();
//		Path logsDir = temporaryFolder.newFolder().toPath();
//		Executor executor = Executors.newCachedThreadPool();
//
//		Eventloop eventloop = Eventloop.getCurrentEventloop();
//
//		DefiningClassLoader classLoader = DefiningClassLoader.create();
//		LocalActiveFs aggregationFs = LocalActiveFs.create(eventloop, executor, aggregationsDir);
//		await(aggregationFs.start());
//		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(),
//				LZ4FrameFormat.create(), aggregationFs);
//		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
//				.withDimension("date", ofLocalDate())
//				.withDimension("advertiser", ofInt())
//				.withDimension("campaign", ofInt())
//				.withDimension("banner", ofInt())
//				.withRelation("campaign", "advertiser")
//				.withRelation("banner", "campaign")
//				.withMeasure("impressions", sum(ofLong()))
//				.withMeasure("clicks", sum(ofLong()))
//				.withMeasure("conversions", sum(ofLong()))
//				.withMeasure("revenue", sum(ofDouble()))
//				.withAggregation(id("detailed")
//						.withDimensions("date", "advertiser", "campaign", "banner")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"))
//				.withAggregation(id("date")
//						.withDimensions("date")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"))
//				.withAggregation(id("advertiser")
//						.withDimensions("advertiser")
//						.withMeasures("impressions", "clicks", "conversions", "revenue"));
//
//		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
//				OT_SYSTEM, LogDiffCodec.create(CubeDiffCodec.create(cube)));
//		initializeRepository(repository);
//
//		OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> uplink = OTUplinkImpl.create(repository, OT_SYSTEM);
//		LogOTState<CubeDiff> logState = LogOTState.create(cube);
//		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(eventloop, OT_SYSTEM, uplink, logState);
//
//		logsFs = LocalActiveFs.create(eventloop, executor, logsDir);
//		await(logsFs.start());
//		BinarySerializer<LogItem> serializer = SerializerBuilder.create(classLoader)
//				.build(LogItem.class);
//		multilog = MultilogImpl.create(eventloop, logsFs, LZ4FrameFormat.create(), serializer, NAME_PARTITION_REMAINDER_SEQ);
//
//		LogOTProcessor<LogItem, CubeDiff> logProcessor = LogOTProcessor.create(
//				eventloop,
//				multilog,
//				cube.logStreamConsumer(LogItem.class),
//				"test",
//				singletonList("partitionA"),
//				logState);
//
//		controller = CubeLogProcessorController.create(
//				eventloop,
//				logState,
//				stateManager,
//				aggregationChunkStorage,
//				singletonList(logProcessor));
//
//
//		await(stateManager.checkout());
//	}
//
//	@Test
//	public void testMalformedLogs() {
//		await(StreamSupplier.of(new LogItem("test")).streamTo(
//				StreamConsumer.ofPromise(multilog.write("partitionA"))));
//
//		Map<String, FileMetadata> files = await(logsFs.list("**"));
//		assertEquals(1, files.size());
//
//
//		String logFile = first(files.keySet());
//		ByteBuf serializedData = await(logsFs.download(logFile).then(supplier -> supplier
//				.transformWith(ChannelFrameDecoder.create(LZ4FrameFormat.create())
//						.withDecoderResets())
//				.toCollector(ByteBufs.collector())));
//
//		// offset right before string
//		int bufSize = serializedData.readRemaining();
//		serializedData.tail(serializedData.head() + 49);
//
//		byte[] malformed = new byte[bufSize - 49];
//		malformed[0] = 127; // exceeds message size
//		await(ChannelSupplier.of(serializedData, ByteBuf.wrapForReading(malformed))
//				.transformWith(ChannelFrameEncoder.create(LZ4FrameFormat.create())
//						.withEncoderResets())
//				.streamTo(logsFs.upload(logFile)));
//
//		CubeException exception = awaitException(controller.process());
//		Throwable firstCause = exception.getCause();
//		assertThat(firstCause, instanceOf(UnknownFormatException.class));
//		assertThat(firstCause.getCause(), instanceOf(StringIndexOutOfBoundsException.class));
//	}
}
