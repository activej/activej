package io.activej.crdt.storage;

import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.cluster.CrdtStorageCluster;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.storage.cluster.DiscoveryService.Partitionings;
import io.activej.crdt.storage.cluster.RendezvousPartitionings;
import io.activej.crdt.storage.cluster.RendezvousPartitionings.Partitioning;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
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

import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CrdtStorageAPITest {
	private static final CrdtDataSerializer<String, TimestampContainer<Integer>> SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));
	private static final CrdtFunction<TimestampContainer<Integer>> CRDT_FUNCTION = TimestampContainer.createCrdtFunction(Integer::max);

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

	private CrdtStorage<String, TimestampContainer<Integer>> client;

	@Before
	public void setup() throws Exception {
		Path folder = temporaryFolder.newFolder().toPath();
		Files.createDirectories(folder);
		client = clientFactory.create(Executors.newSingleThreadExecutor(), folder);
	}

	@FunctionalInterface
	private interface ICrdtClientFactory {

		CrdtStorage<String, TimestampContainer<Integer>> create(Executor executor, Path testFolder) throws Exception;
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
							Map<Integer, CrdtStorage<String, TimestampContainer<Integer>>> map = new HashMap<>();

							int i = 0;
							Set<Integer> partitions = new HashSet<>();
							for (; i < 10; i++) {
								map.put(i, CrdtStorageMap.create(eventloop, CRDT_FUNCTION));
								partitions.add(i);
							}
							Partitioning<Integer> partitioning1 = Partitioning.create(partitions, 3, true, true);

							partitions = new HashSet<>();
							for (; i < 20; i++) {
								map.put(i, CrdtStorageMap.create(eventloop, CRDT_FUNCTION));
								partitions.add(i);
							}
							Partitioning<Integer> partitioning2 = Partitioning.create(partitions, 4, false, true);

							Partitionings<String, TimestampContainer<Integer>, Integer> partitionings = RendezvousPartitionings.create(map, partitioning1, partitioning2);
							DiscoveryService<String, TimestampContainer<Integer>, Integer> discoveryService = DiscoveryService.of(partitionings);
							CrdtStorageCluster<String, TimestampContainer<Integer>, Integer> storageCluster = CrdtStorageCluster.create(eventloop, discoveryService, CRDT_FUNCTION);

							await(storageCluster.start());
							return storageCluster;
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
