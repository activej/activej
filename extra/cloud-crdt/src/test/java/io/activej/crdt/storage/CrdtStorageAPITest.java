package io.activej.crdt.storage;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtTombstone;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.cluster.CrdtStorageCluster;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.storage.cluster.RendezvousPartitionGroup;
import io.activej.crdt.storage.cluster.RendezvousPartitionScheme;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CrdtStorageAPITest {
	private static final CrdtDataSerializer<String, Integer> SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER);
	private static final CrdtFunction<Integer> CRDT_FUNCTION = ignoringTimestamp(Integer::max);

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public ICrdtClientFactory clientFactory;

	private CrdtStorage<String, Integer> client;

	@Before
	public void setup() throws Exception {
		Path folder = temporaryFolder.newFolder().toPath();
		Files.createDirectories(folder);
		client = clientFactory.create(Executors.newSingleThreadExecutor(), folder);
	}

	@FunctionalInterface
	private interface ICrdtClientFactory {
		CrdtStorage<String, Integer> create(Executor executor, Path testFolder) throws Exception;
	}

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{
						"FsCrdtClient",
						(ICrdtClientFactory) (executor, testFolder) -> {
							Eventloop eventloop = Eventloop.getCurrentEventloop();
							LocalActiveFs fs = LocalActiveFs.create(eventloop, executor, testFolder);
							await(fs.start());
							return CrdtStorageFs.create(eventloop, fs, SERIALIZER, CRDT_FUNCTION);
						}
				},
				new Object[]{
						"CrdtStorageMap",
						(ICrdtClientFactory) (executor, testFolder) ->
								CrdtStorageMap.create(Eventloop.getCurrentEventloop(), CRDT_FUNCTION)
				},
				new Object[]{
						"CrdtStorageCluster",
						(ICrdtClientFactory) (executor, testFolder) -> {
							Eventloop eventloop = Eventloop.getCurrentEventloop();
							Map<Integer, CrdtStorage<String, Integer>> map = new HashMap<>();

							int i = 0;
							Set<Integer> partitions = new HashSet<>();
							for (; i < 10; i++) {
								map.put(i, CrdtStorageMap.create(eventloop, CRDT_FUNCTION));
								partitions.add(i);
							}
							RendezvousPartitionGroup<Integer> partitionGroup1 = RendezvousPartitionGroup.create(partitions, 3, true, true);

							partitions = new HashSet<>();
							for (; i < 20; i++) {
								map.put(i, CrdtStorageMap.create(eventloop, CRDT_FUNCTION));
								partitions.add(i);
							}
							RendezvousPartitionGroup<Integer> partitionGroup2 = RendezvousPartitionGroup.create(partitions, 4, false, true);

							RendezvousPartitionScheme<Integer> partitionScheme = RendezvousPartitionScheme.<Integer>create(partitionGroup1, partitionGroup2)
									.withCrdtProvider(map::get);
							DiscoveryService<Integer> discoveryService = DiscoveryService.of(partitionScheme);
							CrdtStorageCluster<String, Integer, Integer> storageCluster = CrdtStorageCluster.create(eventloop, discoveryService, CRDT_FUNCTION);

							await(storageCluster.start());
							return storageCluster;
						}
				}
		);
	}

	@Test
	public void testUploadDownload() {
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 344),
						new CrdtData<>("test_2", 123, 24),
						new CrdtData<>("test_3", 123, -8))
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.of(
						new CrdtData<>("test_2", 123, 44),
						new CrdtData<>("test_3", 123, 74),
						new CrdtData<>("test_4", 123, -28))
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.of(
						new CrdtData<>("test_0", 123, 0),
						new CrdtData<>("test_1", 123, 345),
						new CrdtData<>("test_2", 123, -28))
				.streamTo(StreamConsumer.ofPromise(client.upload())));

		List<CrdtData<String, Integer>> list = await(await(client.download()).toList());
		System.out.println(list);
		assertEquals(Arrays.asList(
						new CrdtData<>("test_0", 123, 0),
						new CrdtData<>("test_1", 123, 345),
						new CrdtData<>("test_2", 123, 44),
						new CrdtData<>("test_3", 123, 74),
						new CrdtData<>("test_4", 123, -28)),
				list);
	}

	@Test
	public void testRemove() {
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 1),
						new CrdtData<>("test_2", 123, 2),
						new CrdtData<>("test_3", 123, 4))
				.streamTo(client.upload()));
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 2),
						new CrdtData<>("test_2", 123, 3),
						new CrdtData<>("test_3", 123, 2))
				.streamTo(client.upload()));
		await(StreamSupplier.of(
						new CrdtTombstone<>("test_2", 124))
				.streamTo(StreamConsumer.ofPromise(client.remove())));

		List<CrdtData<String, Integer>> list = await(await(client.download()).toList());
		System.out.println(list);
		assertEquals(Arrays.asList(
						new CrdtData<>("test_1", 123, 2),
						new CrdtData<>("test_3", 123, 4)),
				list);
	}

	@Test
	public void testUploadAtomicity() {
		List<CrdtData<String, Integer>> expected = Arrays.asList(
				new CrdtData<>("test_1", 123, 344),
				new CrdtData<>("test_2", 123, 44),
				new CrdtData<>("test_3", 123, 74),
				new CrdtData<>("test_4", 123, -28));

		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 344),
						new CrdtData<>("test_2", 123, 24),
						new CrdtData<>("test_3", 123, -8))
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.of(
						new CrdtData<>("test_2", 123, 44),
						new CrdtData<>("test_3", 123, 74),
						new CrdtData<>("test_4", 123, -28))
				.streamTo(StreamConsumer.ofPromise(client.upload())));

		awaitException(StreamSupplier.concat(
						StreamSupplier.of(
								new CrdtData<>("test_0", 123, 0),
								new CrdtData<>("test_1", 123, 345),
								new CrdtData<>("test_2", 123, -28)),
						StreamSupplier.closingWithError(new ExpectedException()))
				.streamTo(client.upload()));

		List<CrdtData<String, Integer>> list = await(await(client.download()).toList());
		System.out.println(list);
		assertEquals(expected, list);
	}

	@Test
	public void testRemoveAtomicity() {
		List<CrdtData<String, Integer>> expected = Arrays.asList(
				new CrdtData<>("test_1", 123, 344),
				new CrdtData<>("test_2", 123, 24),
				new CrdtData<>("test_3", 123, -8));

		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 344),
						new CrdtData<>("test_2", 123, 24),
						new CrdtData<>("test_3", 123, -8))
				.streamTo(StreamConsumer.ofPromise(client.upload())));

		awaitException(StreamSupplier.concat(
						StreamSupplier.of(
								new CrdtTombstone<>("test_1", 124),
								new CrdtTombstone<>("test_2", 124)),
						StreamSupplier.closingWithError(new ExpectedException()))
				.streamTo(StreamConsumer.ofPromise(client.remove())));

		List<CrdtData<String, Integer>> list = await(await(client.download()).toList());
		System.out.println(list);
		assertEquals(expected, list);
	}

	@Test
	public void testUploadTake() {
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 1),
						new CrdtData<>("test_2", 123, 2),
						new CrdtData<>("test_3", 123, 4))
				.streamTo(client.upload()));
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 2),
						new CrdtData<>("test_2", 123, 3),
						new CrdtData<>("test_3", 123, 2))
				.streamTo(client.upload()));
		await(StreamSupplier.of(
						new CrdtTombstone<>("test_2", 124))
				.streamTo(StreamConsumer.ofPromise(client.remove())));

		List<CrdtData<String, Integer>> list = await(await(client.take()).toList());
		assertEquals(Arrays.asList(
						new CrdtData<>("test_1", 123, 2),
						new CrdtData<>("test_3", 123, 4)),
				list);

		assertTrue(await(await(client.download()).toList()).isEmpty());
	}

	@Test
	public void testUploadTakeWithAdditionalData() {
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 1),
						new CrdtData<>("test_2", 123, 2),
						new CrdtData<>("test_3", 123, 4))
				.streamTo(client.upload()));
		await(StreamSupplier.of(
						new CrdtData<>("test_1", 123, 2),
						new CrdtData<>("test_2", 123, 3),
						new CrdtData<>("test_3", 123, 2))
				.streamTo(client.upload()));
		await(StreamSupplier.of(
						new CrdtTombstone<>("test_2", 124))
				.streamTo(StreamConsumer.ofPromise(client.remove())));

		StreamSupplier<CrdtData<String, Integer>> takeSupplier = await(client.take());

		await(StreamSupplier.of(
						new CrdtData<>("test_1", 234, 10),
						new CrdtData<>("test_4", 234, 20))
				.streamTo(client.upload()));

		List<CrdtData<String, Integer>> taken = await(takeSupplier.toList());
		assertEquals(Arrays.asList(
						new CrdtData<>("test_1", 123, 2),
						new CrdtData<>("test_3", 123, 4)),
				taken);

		List<CrdtData<String, Integer>> afterTake = await(await(client.download()).toList());
		assertEquals(Arrays.asList(
						new CrdtData<>("test_1", 234, 10),
						new CrdtData<>("test_4", 234, 20)),
				afterTake);
	}

}
