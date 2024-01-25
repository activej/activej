package io.activej.cube.aggregation;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.AggregationStructure;
import io.activej.cube.CubeState;
import io.activej.cube.CubeStructure;
import io.activej.cube.TestUtils;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.fs.FileSystem;
import io.activej.fs.IFileSystem;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.DescriptionRule;
import io.activej.test.rules.EventloopRule;
import io.minio.*;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static io.activej.cube.TestUtils.stubChunkIdGenerator;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.util.Utils.singlePartition;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class MinioMigrationServiceTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static final String ENDPOINT = "http://127.0.0.1:9000";
	public static final String ACCESS_KEY = "minioadmin";
	public static final String SECRET_KEY = "minioadmin";

	public static final int CHUNK_SIZE = 10;
	public static final String AGGREGATION_ID = "aggregation";

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final DescriptionRule descriptionRule = new DescriptionRule();

	private final DefiningClassLoader classLoader = DefiningClassLoader.create();
	private final CubeStructure structure = CubeStructure.builder()
		.withDimension("key", ofInt())
		.withMeasure("value", sum(ofInt()))
		.withMeasure("timestamp", sum(ofLong()))
		.withAggregation(CubeStructure.AggregationConfig.id(AGGREGATION_ID)
			.withDimensions("key")
			.withMeasures("value", "timestamp")
		)
		.build();

	private Reactor reactor;
	private Executor executor;

	private IFileSystem fileSystem;
	private AggregationChunkStorage fromStorage;

	private String bucket;
	private MinioChunkStorage toStorage;
	private MinioAsyncClient client;

	@Before
	public void setUp() throws Exception {
		client = MinioAsyncClient.builder()
			.endpoint(ENDPOINT)
			.credentials(ACCESS_KEY, SECRET_KEY)
			.build();

		Description description = descriptionRule.getDescription();
		bucket = (description.getTestClass().getSimpleName() + "-" + descriptionRule.getDescription().getMethodName()).toLowerCase();

		clearBucket();

		client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build()).get();

		reactor = Reactor.getCurrentReactor();

		executor = Executors.newSingleThreadExecutor();

		toStorage = MinioChunkStorage.create(
			reactor,
			new ChunkIdGenerator() {
				@Override
				public Promise<String> createProtoChunkId() {
					throw new AssertionError();
				}

				@Override
				public Promise<Map<String, Long>> convertToActualChunkIds(Set<String> protoChunkIds) {
					throw new AssertionError();
				}
			},
			client,
			executor,
			bucket
		);

		FileSystem fileSystem = FileSystem.create(reactor, executor, temporaryFolder.newFolder().toPath());
		await(fileSystem.start());
		this.fileSystem = fileSystem;

		fromStorage = AggregationChunkStorage.create(
			reactor,
			stubChunkIdGenerator(),
			FrameFormats.lz4(),
			fileSystem
		);
	}

	@After
	public void tearDown() throws Exception {
		clearBucket();
	}

	@Test
	public void testMigration() throws ExecutionException, InterruptedException {
		int nObjects = 1_000;
		AggregationChunker<KeyValuePair> chunker = createChunker();

		Set<KeyValuePair> expected = generateItems(nObjects);
		StreamSupplier<KeyValuePair> supplier = StreamSuppliers.ofIterable(expected);

		assertTrue(await(fromStorage.listChunks()).isEmpty());

		await(supplier.streamTo(chunker));

		await(fromStorage.finish(getProtoChunks()));

		Set<Long> fromChunks = await(fromStorage.listChunks());
		assertChunks(fromStorage, expected, fromChunks);

		assertTrue(await(toStorage.listChunks()).isEmpty());

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = TestUtils.stubStateManager(structure);
		CubeDiff cubeDiff = CubeDiff.of(Map.of(AGGREGATION_ID, AggregationDiff.of(idsToChunks(fromChunks), Set.of())));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(cubeDiff))));

		CompletableFuture<Void> migrateFuture = MinioMigrationService.migrate(reactor, executor, stateManager, fileSystem, client, bucket);
		await();
		migrateFuture.get();

		Set<Long> toChunks = await(toStorage.listChunks());
		assertEquals(fromChunks, toChunks);

		assertChunks(toStorage, expected, toChunks);
	}

	private static Set<AggregationChunk> idsToChunks(Set<Long> fromChunks) {
		return fromChunks.stream()
			.map(chunkId -> AggregationChunk.create(chunkId,
				List.of("value", "timestamp"),
				PrimaryKey.ofList(List.of(0, 0L)),
				PrimaryKey.ofList(List.of(1, 1L)),
				10))
			.collect(toSet());
	}

	private static Set<KeyValuePair> generateItems(int nObjects) {
		Random random = ThreadLocalRandom.current();

		return Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
			.limit(nObjects)
			.collect(toSet());
	}

	private void assertChunks(IAggregationChunkStorage storage, Set<KeyValuePair> expected, Set<Long> chunks) {
		Set<KeyValuePair> result = new HashSet<>(expected.size());
		AggregationStructure aggregationStructure = structure.getAggregationStructure(AGGREGATION_ID);
		for (Long chunkId : chunks) {
			List<String> fields = aggregationStructure.getMeasures();
			List<KeyValuePair> readItems = await(storage.read(aggregationStructure, fields, KeyValuePair.class, chunkId, classLoader)
				.then(StreamSupplier::toList));
			result.addAll(readItems);
		}

		assertEquals(expected, result);
	}

	private AggregationChunker<KeyValuePair> createChunker() {
		AggregationStructure aggregationStructure = structure.getAggregationStructure(AGGREGATION_ID);
		return AggregationChunker.create(
			aggregationStructure, aggregationStructure.getMeasures(), KeyValuePair.class, singlePartition(),
			fromStorage, classLoader, CHUNK_SIZE);
	}

	private Set<String> getProtoChunks() {
		return await(fileSystem.list("*" + AggregationChunkStorage.TEMP_LOG)).keySet().stream()
			.map(fileName -> fileName.substring(0, fileName.length() - AggregationChunkStorage.TEMP_LOG.length()))
			.collect(toSet());
	}

	private void clearBucket() throws Exception {
		if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build()).get()) {
			return;
		}

		Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder().bucket(bucket).build());
		Set<String> objects = new HashSet<>();
		for (Result<Item> result : results) {
			objects.add(result.get().objectName());
		}

		Iterable<Result<DeleteError>> deleteResults = client.removeObjects(
			RemoveObjectsArgs.builder()
				.bucket(bucket)
				.objects(objects.stream().map(DeleteObject::new).toList())
				.build()
		);
		for (Result<DeleteError> deleteResult : deleteResults) {
			deleteResult.get();
		}

		client.removeBucket(RemoveBucketArgs.builder().bucket(bucket).build());
	}
}
