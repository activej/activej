package io.activej.cube.aggregation;

import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.cube.AggregationStructure;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
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
import org.junit.runner.Description;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.activej.cube.TestUtils.aggregationStructureBuilder;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.util.Utils.singlePartition;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class MinioChunkStorageTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public static final String ENDPOINT = "http://127.0.0.1:9000";
	public static final String ACCESS_KEY = "minioadmin";
	public static final String SECRET_KEY = "minioadmin";

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public final DescriptionRule descriptionRule = new DescriptionRule();

	private final DefiningClassLoader classLoader = DefiningClassLoader.create();
	private final AggregationStructure structure = aggregationStructureBuilder()
		.withKey("key", ofInt())
		.withMeasure("value", sum(ofInt()))
		.withMeasure("timestamp", sum(ofLong()))
		.build();

	private String bucket;
	private MinioChunkStorage storage;
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

		storage = MinioChunkStorage.create(
				Reactor.getCurrentReactor(),
				AsyncSupplier.of(new RefLong(0)::inc),
				client,
				Executors.newSingleThreadExecutor(),
				bucket
			);
	}

	@After
	public void tearDown() throws Exception {
		clearBucket();
	}

	@Test
	public void testAcknowledge() {
		int nChunks = 100;
		AggregationChunker<KeyValuePair> chunker = createChunker(1);

		Set<String> expected = IntStream.range(0, nChunks + 1).mapToObj(i -> i + 1 + MinioChunkStorage.LOG).collect(toSet());

		Random random = ThreadLocalRandom.current();
		StreamSupplier<KeyValuePair> supplier = StreamSuppliers.ofStream(
			Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
				.limit(nChunks));

		Set<String> objectNames = await(supplier.streamTo(chunker)
			.map($ -> {
				Iterable<Result<Item>> results = client.listObjects(ListObjectsArgs.builder()
					.bucket(bucket)
					.build());
				Set<String> objects = new HashSet<>();
				for (Result<Item> result : results) {
					objects.add(result.get().objectName());
				}
				return objects;
			}));

		assertEquals(expected, objectNames);
	}

	@Test
	public void testChunkWrite() {
		int nObjects = 99;
		int chunkSize = 10;
		AggregationChunker<KeyValuePair> chunker = createChunker(chunkSize);

		Random random = ThreadLocalRandom.current();
		List<KeyValuePair> items = Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
			.limit(nObjects)
			.toList();
		StreamSupplier<KeyValuePair> supplier = StreamSuppliers.ofIterable(items);

		await(supplier.streamTo(chunker));

		Set<Long> chunks = await(storage.listChunks());

		assertEquals(LongStream.range(1, 11).boxed().collect(Collectors.toSet()), chunks);
	}

	@Test
	public void testChunkRead() {
		int nObjects = 1000;
		int chunkSize = 10;
		AggregationChunker<KeyValuePair> chunker = createChunker(chunkSize);

		Random random = ThreadLocalRandom.current();
		List<KeyValuePair> items = Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
			.limit(nObjects)
			.toList();
		StreamSupplier<KeyValuePair> supplier = StreamSuppliers.ofIterable(items);

		await(supplier.streamTo(chunker));

		Set<Long> chunks = await(storage.listChunks());

		List<KeyValuePair> result = new ArrayList<>(items.size());
		for (Long chunkId : chunks) {
			List<KeyValuePair> readItems = await(storage.read(structure, structure.getMeasures(), KeyValuePair.class, chunkId, classLoader).then(StreamSupplier::toList));
			result.addAll(readItems);
		}

		assertEquals(items, result);
	}

	@Test
	public void testChunkDeletion() {
		int nObjects = 1000;
		int chunkSize = 10;
		AggregationChunker<KeyValuePair> chunker = createChunker(chunkSize);

		Random random = ThreadLocalRandom.current();
		List<KeyValuePair> items = Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
			.limit(nObjects)
			.toList();
		StreamSupplier<KeyValuePair> supplier = StreamSuppliers.ofIterable(items);

		await(supplier.streamTo(chunker));

		Set<Long> chunks = await(storage.listChunks());

		Set<Long> preserved = new HashSet<>();
		Set<Long> deleted = new HashSet<>();
		for (Long chunk : chunks) {
			if (random.nextBoolean()) {
				preserved.add(chunk);
			} else {
				deleted.add(chunk);
			}
		}

		await(storage.deleteChunks(deleted));

		assertEquals(preserved, await(storage.listChunks()));
	}

	private AggregationChunker<KeyValuePair> createChunker(int chunkSize) {
		return AggregationChunker.create(
			structure, structure.getMeasures(), KeyValuePair.class, singlePartition(),
			storage, classLoader, chunkSize);
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
