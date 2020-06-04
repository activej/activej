package io.activej.crdt.storage;

import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.storage.local.CrdtStorageRocksDB;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.remotefs.LocalFsClient;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CrdtStorageAPITest {
	private static final CrdtDataSerializer<String, TimestampContainer<Integer>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public ICrdtClientFactory<String, TimestampContainer<Integer>> clientFactory;

	private CrdtStorage<String, TimestampContainer<Integer>> client;

	@Before
	public void setup() throws Exception {
		Path folder = temporaryFolder.newFolder().toPath();
		Files.createDirectories(folder);
		client = clientFactory.create(Executors.newSingleThreadExecutor(), folder, TimestampContainer.createCrdtFunction(Integer::max));
	}

	@After
	public void tearDown() {
		if (client instanceof CrdtStorageRocksDB) {
//			((RocksDBCrdtClient) client).getDb().close();
		}
	}

	@FunctionalInterface
	private interface ICrdtClientFactory<K extends Comparable<K>, S> {

		CrdtStorage<K, S> create(Executor executor, Path testFolder, CrdtFunction<S> crdtFunction) throws Exception;
	}

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{
						"FsCrdtClient",
						(ICrdtClientFactory<String, TimestampContainer<Integer>>) (executor, testFolder, crdtFunction) -> {
							Eventloop eventloop = Eventloop.getCurrentEventloop();
							return CrdtStorageFs.create(eventloop, LocalFsClient.create(eventloop, executor, testFolder), serializer, crdtFunction);
						}
				},
				new Object[]{
						"RocksDBCrdtClient",
						(ICrdtClientFactory<String, TimestampContainer<Integer>>) (executor, testFolder, crdtFunction) -> {
							Options options = new Options()
									.setCreateIfMissing(true)
									.setComparator(new CrdtStorageRocksDB.KeyComparator<>(UTF8_SERIALIZER));
							RocksDB rocksdb = RocksDB.open(options, testFolder.resolve("rocksdb").toString());
							return CrdtStorageRocksDB.create(Eventloop.getCurrentEventloop(), executor, rocksdb, serializer, crdtFunction);
						}
				}
		);
	}

	@Test
	public void testUploadDownload() {
		List<CrdtData<String, TimestampContainer<Integer>>> expected = Arrays.asList(
				new CrdtData<>("test_0", new TimestampContainer<>(123, 0)),
				new CrdtData<>("test_1", new TimestampContainer<>(123, 345)),
				new CrdtData<>("test_2", new TimestampContainer<>(123, 44)),
				new CrdtData<>("test_3", new TimestampContainer<>(123, 74)),
				new CrdtData<>("test_4", new TimestampContainer<>(123, -28))
		);

		await(StreamSupplier.of(
				new CrdtData<>("test_1", new TimestampContainer<>(123, 344)),
				new CrdtData<>("test_2", new TimestampContainer<>(123, 24)),
				new CrdtData<>("test_3", new TimestampContainer<>(123, -8))).streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.of(
				new CrdtData<>("test_2", new TimestampContainer<>(123, 44)),
				new CrdtData<>("test_3", new TimestampContainer<>(123, 74)),
				new CrdtData<>("test_4", new TimestampContainer<>(123, -28))).streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.of(
				new CrdtData<>("test_0", new TimestampContainer<>(123, 0)),
				new CrdtData<>("test_1", new TimestampContainer<>(123, 345)),
				new CrdtData<>("test_2", new TimestampContainer<>(123, -28))).streamTo(StreamConsumer.ofPromise(client.upload())));

		List<CrdtData<String, TimestampContainer<Integer>>> list = await(await(client.download()).toList());
		System.out.println(list);
		assertEquals(expected, list);
	}

	@Test
	public void testDelete() {
		List<CrdtData<String, TimestampContainer<Integer>>> expected = Arrays.asList(
				new CrdtData<>("test_1", new TimestampContainer<>(123, 2)),
				new CrdtData<>("test_3", new TimestampContainer<>(123, 4))
		);
		await(StreamSupplier.of(
				new CrdtData<>("test_1", new TimestampContainer<>(123, 1)),
				new CrdtData<>("test_2", new TimestampContainer<>(123, 2)),
				new CrdtData<>("test_3", new TimestampContainer<>(123, 4))).streamTo(client.upload()));
		await(StreamSupplier.of(
				new CrdtData<>("test_1", new TimestampContainer<>(123, 2)),
				new CrdtData<>("test_2", new TimestampContainer<>(123, 3)),
				new CrdtData<>("test_3", new TimestampContainer<>(123, 2))).streamTo(client.upload()));
		await(StreamSupplier.of("test_2").streamTo(StreamConsumer.ofPromise(client.remove())));

		List<CrdtData<String, TimestampContainer<Integer>>> list = await(await(client.download()).toList());
		System.out.println(list);
		assertEquals(expected, list);
	}
}
