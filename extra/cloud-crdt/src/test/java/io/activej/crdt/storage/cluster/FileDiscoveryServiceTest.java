package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.crdt.storage.cluster.DiscoveryService.PartitionScheme;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public class FileDiscoveryServiceTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private Path file;
	private FileDiscoveryService discoveryService;
	private WatchService watchService;

	private static final byte[] TEST_PARTITIONS_1 = ("[" +
			"    {" +
			"        \"ids\": [" +
			"            {" +
			"                \"id\": \"a\"," +
			"                \"crdtAddress\": \"localhost:9001\"," +
			"                \"rpcAddress\": \"localhost:9051\"" +
			"            }," +
			"            {" +
			"                \"id\": \"b\"," +
			"                \"crdtAddress\": \"localhost:9002\"," +
			"                \"rpcAddress\": \"localhost:9052\"" +
			"            }" +
			"        ]," +
			"        \"replicaCount\": 2," +
			"        \"repartition\": true," +
			"        \"active\": true" +
			"    }," +
			"    {" +
			"        \"ids\": [" +
			"            {" +
			"                \"id\": \"c\"," +
			"                \"crdtAddress\": \"localhost:9003\"," +
			"                \"rpcAddress\": \"localhost:9053\"" +
			"            }" +
			"        ]," +
			"        \"replicaCount\": 1," +
			"        \"repartition\": false," +
			"        \"active\": false" +
			"    }" +
			"]").getBytes(UTF_8);

	private static final byte[] TEST_PARTITIONS_2 = ("[" +
			"    {" +
			"        \"ids\": [" +
			"            {" +
			"                \"id\": \"a\"," +
			"                \"crdtAddress\": \"localhost:9001\"," +
			"                \"rpcAddress\": \"localhost:9051\"" +
			"            }" +
			"        ]," +
			"        \"replicaCount\": 1," +
			"        \"repartition\": true," +
			"        \"active\": true" +
			"    }," +
			"    {" +
			"        \"ids\": [" +
			"            {" +
			"                \"id\": \"b\"," +
			"                \"crdtAddress\": \"localhost:9002\"," +
			"                \"rpcAddress\": \"localhost:9052\"" +
			"            }" +
			"        ]," +
			"        \"replicaCount\": 1," +
			"        \"repartition\": false," +
			"        \"active\": false" +
			"    }" +
			"]").getBytes(UTF_8);

	private static final byte[] TEST_PARTITIONS_3 = ("[" +
			"    {" +
			"        \"ids\": [" +
			"            {" +
			"                \"id\": \"a\"," +
			"                \"crdtAddress\": \"localhost:9001\"," +
			"                \"rpcAddress\": \"localhost:9051\"" +
			"            }," +
			"            {" +
			"                \"id\": \"b\"," +
			"                \"crdtAddress\": \"localhost:9002\"," +
			"                \"rpcAddress\": \"localhost:9052\"" +
			"            }" +
			"        ]," +
			"        \"replicaCount\": 1," +
			"        \"repartition\": false," +
			"        \"active\": true" +
			"    }" +
			"]").getBytes(UTF_8);

	private static final byte[] TEST_PARTITIONS_4 = ("[" +
			"    {" +
			"        \"ids\": [" +
			"            {" +
			"                \"id\": \"a\"," +
			"                \"crdtAddress\": \"localhost:9001\"," +
			"                \"rpcAddress\": \"localhost:9051\"" +
			"            }," +
			"            {" +
			"                \"id\": \"d\"," +
			"                \"crdtAddress\": \"localhost:9004\"," +
			"                \"rpcAddress\": \"localhost:9054\"" +
			"            }" +
			"        ]," +
			"        \"replicaCount\": 1," +
			"        \"repartition\": false," +
			"        \"active\": true" +
			"    }" +
			"]").getBytes(UTF_8);

	@Before
	public void setUp() throws Exception {
		file = temporaryFolder.newFile().toPath();
		Files.write(file, "[]".getBytes(UTF_8));
		watchService = file.getFileSystem().newWatchService();
		discoveryService = FileDiscoveryService.create(Eventloop.getCurrentEventloop(), watchService, file);
	}

	@Test
	public void testEmptyFileChange() throws IOException {
		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme = await(supplier.get());

		assertTrue(scheme.getPartitions().isEmpty());

		Promise<PartitionScheme<PartitionId>> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		Files.write(file, TEST_PARTITIONS_1);

		PartitionScheme<PartitionId> newScheme = await(nextPromise);
		assertTestPartitions1(newScheme);
	}

	@Test
	public void testNonEmptyFileChange() throws IOException {
		Files.write(file, TEST_PARTITIONS_1);

		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme1 = await(supplier.get());

		assertTestPartitions1(scheme1);

		Promise<PartitionScheme<PartitionId>> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		Files.write(file, TEST_PARTITIONS_2);

		PartitionScheme<PartitionId> scheme2 = await(nextPromise);
		assertTestPartitions2(scheme2);
	}

	@Test
	public void testFileChangeBetweenGet() throws IOException, InterruptedException {
		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme = await(supplier.get());
		assertTrue(scheme.getPartitions().isEmpty());

		Files.write(file, TEST_PARTITIONS_1);

		Thread.sleep(200);

		PartitionScheme<PartitionId> nextScheme = await(supplier.get());
		assertTestPartitions1(nextScheme);
	}

	@Test
	public void testFileChangeMultipleTimesBetweenGet() throws IOException, InterruptedException {
		Files.write(file, TEST_PARTITIONS_1);

		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme1 = await(supplier.get());
		assertTestPartitions1(scheme1);

		Files.write(file, TEST_PARTITIONS_2);

		Thread.sleep(200);

		Files.write(file, TEST_PARTITIONS_3);

		Thread.sleep(200);

		PartitionScheme<PartitionId> nextScheme = await(supplier.get());
		assertTestPartitions3(nextScheme);
	}

	@Test
	public void testPartitionChange() throws IOException {
		discoveryService.withCrdtProvider(partitionId -> CrdtStorageMap.create(Eventloop.getCurrentEventloop()));

		Files.write(file, TEST_PARTITIONS_1);

		CrdtStorageCluster<String, Integer, PartitionId> cluster = CrdtStorageCluster.create(Eventloop.getCurrentEventloop(), discoveryService, ignoringTimestamp(Integer::max));

		await(cluster.start()
				.whenResult(() -> assertEquals(Set.of("a", "b", "c"), cluster.getCrdtStorages().keySet()
						.stream()
						.map(PartitionId::getId)
						.collect(toSet())))
				.whenResult(() -> Files.write(file, TEST_PARTITIONS_4))
				.then(() -> Promises.delay(Duration.ofMillis(200)))
				.whenResult(() -> assertEquals(Set.of("a", "d"), cluster.getCrdtStorages().keySet()
						.stream()
						.map(PartitionId::getId)
						.collect(toSet())))
				.then(() -> {
					watchService.close();
					return cluster.stop();
				}));
	}

	private void assertTestPartitions1(PartitionScheme<PartitionId> partitionScheme) {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = ((RendezvousPartitionScheme<PartitionId>) partitionScheme).getPartitionGroups();
		assertEquals(2, partitionGroups.size());
		RendezvousPartitionGroup<PartitionId> group1 = partitionGroups.get(0);
		assertEquals(2, group1.getReplicaCount());
		assertTrue(group1.isRepartition());
		assertTrue(group1.isActive());
		assertEquals(Set.of(
				PartitionId.of("a", new InetSocketAddress("localhost", 9001), new InetSocketAddress("localhost", 9051)),
				PartitionId.of("b", new InetSocketAddress("localhost", 9002), new InetSocketAddress("localhost", 9052))
		), group1.getPartitionIds());

		RendezvousPartitionGroup<PartitionId> group2 = partitionGroups.get(1);
		assertEquals(1, group2.getReplicaCount());
		assertFalse(group2.isRepartition());
		assertFalse(group2.isActive());
		assertEquals(Set.of(
				PartitionId.of("c", new InetSocketAddress("localhost", 9003), new InetSocketAddress("localhost", 9053))
		), group2.getPartitionIds());
	}

	private void assertTestPartitions2(PartitionScheme<PartitionId> partitionScheme) {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = ((RendezvousPartitionScheme<PartitionId>) partitionScheme).getPartitionGroups();
		assertEquals(2, partitionGroups.size());
		RendezvousPartitionGroup<PartitionId> group1 = partitionGroups.get(0);
		assertEquals(1, group1.getReplicaCount());
		assertTrue(group1.isRepartition());
		assertTrue(group1.isActive());
		assertEquals(Set.of(
				PartitionId.of("a", new InetSocketAddress("localhost", 9001), new InetSocketAddress("localhost", 9051))
		), group1.getPartitionIds());

		RendezvousPartitionGroup<PartitionId> group2 = partitionGroups.get(1);
		assertEquals(1, group2.getReplicaCount());
		assertFalse(group2.isRepartition());
		assertFalse(group2.isActive());
		assertEquals(Set.of(
				PartitionId.of("b", new InetSocketAddress("localhost", 9002), new InetSocketAddress("localhost", 9052))
		), group2.getPartitionIds());
	}

	private void assertTestPartitions3(PartitionScheme<PartitionId> partitionScheme) {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = ((RendezvousPartitionScheme<PartitionId>) partitionScheme).getPartitionGroups();
		assertEquals(1, partitionGroups.size());
		RendezvousPartitionGroup<PartitionId> group1 = partitionGroups.get(0);
		assertEquals(1, group1.getReplicaCount());
		assertFalse(group1.isRepartition());
		assertTrue(group1.isActive());
		assertEquals(Set.of(
				PartitionId.of("a", new InetSocketAddress("localhost", 9001), new InetSocketAddress("localhost", 9051)),
				PartitionId.of("b", new InetSocketAddress("localhost", 9002), new InetSocketAddress("localhost", 9052))
		), group1.getPartitionIds());
	}

}
