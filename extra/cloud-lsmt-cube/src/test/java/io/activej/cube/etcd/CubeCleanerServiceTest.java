package io.activej.cube.etcd;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.FatalErrorHandlers;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.AggregationStructure;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.ChunkIdGenerator;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.LogDiff;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileSystem;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.DescriptionRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.etcd.CubeCleanerService.DEFAULT_CLEANUP_OLDER_THAN;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.time.TestCurrentTimeProvider.*;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("DataFlowIssue")
public class CubeCleanerServiceTest {
	private static final String AGGREGATION_ID = "pub";
	private static final Client ETCD_CLIENT = Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();
	private static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public DescriptionRule descriptionRule = new DescriptionRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private CubeEtcdStateManager stateManager;
	private AggregationChunkStorage aggregationChunkStorage;

	private final SettableCurrentTimeProvider now = settable(ofConstant(100L));

	private CubeStructure structure;
	private CubeCleanerService cleanerService;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Before
	public void setUp() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		Reactor reactor = Eventloop.builder()
			.withFatalErrorHandler(FatalErrorHandlers.rethrow())
			.withCurrentThread()
			.withTimeProvider(now)
			.build();

		FileSystem fileSystem = FileSystem.create(reactor, executor, aggregationsDir);
		await(fileSystem.start());
		aggregationChunkStorage = AggregationChunkStorage.create(reactor, new ChunkIdGenerator() {
				@Override
				public Promise<String> createProtoChunkId() {
					throw new AssertionError();
				}

				@Override
				public Promise<Map<String, Long>> convertToActualChunkIds(Set<String> protoChunkIds) {
					return Promise.of(protoChunkIds.stream().collect(toMap(Function.identity(), Long::parseLong)));
				}
			},
			FrameFormats.lz4(), fileSystem);

		structure = CubeStructure.builder()
			.withDimension("pub", ofInt())
			.withMeasure("pubRequests", sum(ofLong()))
			.withAggregation(id(AGGREGATION_ID)
				.withDimensions("pub")
				.withMeasures("pubRequests"))
			.build();

		JsonCodec<PrimaryKey> primaryKeyCodec = JsonCodecs.ofList((JsonCodec<Object>) (JsonCodec) JsonCodecs.ofLong())
			.transform(PrimaryKey::values, PrimaryKey::ofList);

		Description description = descriptionRule.getDescription();
		ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());

		stateManager = CubeEtcdStateManager.builder(ETCD_CLIENT, root, structure)
			.withCurrentTimeProvider(now)
			.withChunkCodecsFactory($ -> new AggregationChunkJsonEtcdKVCodec(primaryKeyCodec))
			.build();
		stateManager.delete();

		stateManager.start();

		cleanerService = CubeCleanerService.builder(ETCD_CLIENT, aggregationChunkStorage, root)
			.withCurrentTimeProvider(now)
			.build();

		await(cleanerService.start());

		assertTrue(getStorageChunks().isEmpty());
	}

	@After
	public void tearDown() {
		await(cleanerService.stop());
	}

	@Test
	public void testCleanup() {
		processChunks(100, Set.of(1L, 2L, 3L), Set.of());
		processChunks(200, Set.of(4L, 5L, 6L), Set.of(1L, 2L, 3L));
		processChunks(300, Set.of(7L, 8L, 9L), Set.of(4L, 5L));

		assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), getStorageChunks());

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 50);
		assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), getStorageChunks());

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 150);
		assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), getStorageChunks());

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 250);
		assertEquals(Set.of(4L, 5L, 6L, 7L, 8L, 9L), getStorageChunks());

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 350);
		assertEquals(Set.of(6L, 7L, 8L, 9L), getStorageChunks());
	}

	@Test
	public void testCleanupWithAlreadyDeletedChunks() {
		processChunks(100, Set.of(1L, 2L, 3L), Set.of());
		processChunks(200, Set.of(4L, 5L, 6L), Set.of(1L, 2L, 3L));

		assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L), getStorageChunks());

		await(aggregationChunkStorage.deleteChunks(Set.of(1L, 2L)));

		assertEquals(Set.of(3L, 4L, 5L, 6L), getStorageChunks());

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 250);
		assertEquals(Set.of(4L, 5L, 6L), getStorageChunks());
	}

	@Test
	public void testCleanupAllDeletedChunks() {
		processChunks(100, Set.of(1L, 2L, 3L), Set.of());
		processChunks(200, Set.of(4L, 5L, 6L), Set.of(1L, 2L, 3L));
		processChunks(300, Set.of(), Set.of(4L, 5L, 6L));

		assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L), getStorageChunks());

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 350);
		assertTrue(getStorageChunks().isEmpty());
	}

	@Test
	public void testCleanupAutoScheduling() {
		processChunks(100, Set.of(1L, 2L, 3L), Set.of());
		processChunks(200, Set.of(4L, 5L, 6L), Set.of(1L, 2L, 3L));
		processChunks(300, Set.of(), Set.of(4L, 5L, 6L));

		cleanupAt(DEFAULT_CLEANUP_OLDER_THAN.toMillis() + 250);
		assertEquals(Set.of(4L, 5L, 6L), getStorageChunks());

		Eventloop eventloop = Reactor.getCurrentReactor();
		eventloop.keepAlive(true);
		now.setTimeProvider(ofTimeSequence(now.currentTimeMillis() - 100, 100));
		try {
			await(cleanerService.cleanup()
				.then(() -> aggregationChunkStorage.list(aLong -> true, a -> true))
				.whenResult(chunks -> {
					assertEquals(Set.of(4L, 5L, 6L), chunks);
					eventloop.keepAlive(false);
				}));
		} finally {
			eventloop.keepAlive(false);
		}

		assertTrue(getStorageChunks().isEmpty());
	}

	@Test
	public void testCleanupIterations() {
		processChunks(100, Set.of(1L, 2L, 3L), Set.of());
		processChunks(200, Set.of(4L, 5L, 6L), Set.of(1L, 2L, 3L));

		Eventloop eventloop = Reactor.getCurrentReactor();
		eventloop.keepAlive(true);
		now.setTimeProvider(ofTimeSequence(now.currentTimeMillis() + DEFAULT_CLEANUP_OLDER_THAN.toMillis() - 1000, 100));
		await(aggregationChunkStorage.listChunks()
			.whenResult(chunks -> assertEquals(6, chunks.size()))
			.then(() -> Promises.<Set<Long>>until(Set.of(), chunks ->
				aggregationChunkStorage.listChunks(), s -> s.size() == 3))
			.then(() -> processChunksAsync(300, Set.of(), Set.of(4L, 5L, 6L)))
			.then(() -> Promises.<Set<Long>>until(Set.of(), chunks ->
				aggregationChunkStorage.listChunks(), Set::isEmpty))
			.whenComplete(() -> eventloop.keepAlive(false)));
	}

	private void processChunks(long timestamp, Set<Long> addedChunks, Set<Long> removedChunks) {
		now.setTimeProvider(ofConstant(timestamp));

		await(stateManager.push(List.of(LogDiff.forCurrentPosition(CubeDiff.of(
			Map.of(AGGREGATION_ID, AggregationDiff.of(idsToChunks(addedChunks), idsToChunks(removedChunks)))
		)))));

		AggregationStructure aggregationStructure = structure.getAggregationStructure(AGGREGATION_ID);

		Set<String> protoChunkIds = new HashSet<>(addedChunks.size());
		for (Long addedChunk : addedChunks) {
			String protoChunkId = String.valueOf(addedChunk);
			protoChunkIds.add(protoChunkId);
			Promise<StreamConsumer<Container>> writePromise = aggregationChunkStorage.write(
				aggregationStructure,
				List.of(),
				Container.class,
				protoChunkId,
				CLASS_LOADER
			);
			await(StreamSuppliers.<Container>ofValues().streamTo(writePromise));
		}

		await(aggregationChunkStorage.finish(protoChunkIds));
	}

	private Promise<Void> processChunksAsync(long timestamp, Set<Long> addedChunks, Set<Long> removedChunks) {
		CurrentTimeProvider prevProvider = now.getTimeProvider();
		now.setTimeProvider(ofConstant(timestamp));
		AggregationStructure aggregationStructure = structure.getAggregationStructure(AGGREGATION_ID);

		Set<String> protoChunkIds = new HashSet<>(addedChunks.size());
		return stateManager.push(List.of(LogDiff.forCurrentPosition(CubeDiff.of(
				Map.of(AGGREGATION_ID, AggregationDiff.of(idsToChunks(addedChunks), idsToChunks(removedChunks)))
			))))
			.then(() -> Promises.all(addedChunks.stream()
				.map(addedChunk -> {
					String protoChunkId = String.valueOf(addedChunk);
					protoChunkIds.add(protoChunkId);
					return StreamSuppliers.<Container>ofValues().streamTo(aggregationChunkStorage.write(
						aggregationStructure,
						List.of(),
						Container.class,
						protoChunkId,
						CLASS_LOADER
					));
				})))
			.then(() -> aggregationChunkStorage.finish(protoChunkIds))
			.whenComplete(() -> now.setTimeProvider(prevProvider))
			.toVoid();
	}

	private static Set<AggregationChunk> idsToChunks(Set<Long> addedChunks) {
		return addedChunks.stream()
			.map(id -> AggregationChunk.create(id, List.of(), PrimaryKey.ofArray(0L), PrimaryKey.ofArray(0L), 1))
			.collect(Collectors.toSet());
	}

	private void cleanupAt(long timestamp) {
		now.setTimeProvider(ofConstant(timestamp));
		try {
			// waiting for etcd watcher to catch up
			Thread.sleep(100);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		await(cleanerService.cleanup());
	}

	private Set<Long> getStorageChunks() {
		return await(aggregationChunkStorage.list(aLong -> true, a -> true));
	}

	@SuppressWarnings("unused")
	public static final class Container {
		public int pub;
	}
}
