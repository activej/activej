package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.time.Stopwatch;
import io.activej.crdt.storage.cluster.DiscoveryService.PartitionScheme;
import io.activej.eventloop.Eventloop;
import io.activej.fs.cluster.EtcdWatchService;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class EtcdDiscoveryServiceTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private EtcdClusterExtension extension;
	private Client etcdClient;
	private EtcdDiscoveryService discoveryService;
	private ByteSequence bs;

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

	@Before
	public void setUp() throws Exception {
		extension = EtcdClusterExtension.builder().withNodes(3).build();
		Stopwatch started = Stopwatch.createStarted();
		extension.beforeEach(null);
		started.reset();
		started.start();

		EtcdCluster etcdCluster = extension.cluster();
		etcdCluster.start();
		byte[] keyBytes = new byte[100];
		ThreadLocalRandom.current().nextBytes(keyBytes);
		String key = "test";
		bs = ByteSequence.from(key, UTF_8);
		etcdClient = Client.builder().target("cluster://" + etcdCluster.clusterName()).build();
		putValue("[]".getBytes(UTF_8));
		EtcdWatchService watchService = EtcdWatchService.create(Eventloop.getCurrentEventloop(), etcdClient, key);
		discoveryService = EtcdDiscoveryService.create(watchService);
	}

	@After
	public void tearDown() throws Exception {
		etcdClient.close();
		extension.afterEach(null);
	}

	@Test
	public void testEmptyFileChange() {
		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme = await(supplier.get());

		assertTrue(scheme.getPartitions().isEmpty());

		Promise<PartitionScheme<PartitionId>> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		putValue(TEST_PARTITIONS_1);

		PartitionScheme<PartitionId> newScheme = await(nextPromise);
		assertTestPartitions1(newScheme);
	}

	@Test
	public void testNonEmptyFileChange() {
		putValue(TEST_PARTITIONS_1);

		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme1 = await(supplier.get());

		assertTestPartitions1(scheme1);

		Promise<PartitionScheme<PartitionId>> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		putValue(TEST_PARTITIONS_2);

		PartitionScheme<PartitionId> scheme2 = await(nextPromise);
		assertTestPartitions2(scheme2);
	}

	@Test
	public void testFileChangeBetweenGet() {
		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme = await(supplier.get());
		assertTrue(scheme.getPartitions().isEmpty());

		putValue(TEST_PARTITIONS_1);

		PartitionScheme<PartitionId> nextScheme = await(supplier.get());
		assertTestPartitions1(nextScheme);
	}

	@Test
	public void testFileChangeMultipleTimesBetweenGet() throws InterruptedException {
		putValue(TEST_PARTITIONS_1);

		AsyncSupplier<PartitionScheme<PartitionId>> supplier = discoveryService.discover();

		PartitionScheme<PartitionId> scheme1 = await(supplier.get());
		assertTestPartitions1(scheme1);

		putValue(TEST_PARTITIONS_2);
		putValue(TEST_PARTITIONS_3);

		Thread.sleep(100);

		PartitionScheme<PartitionId> nextScheme = await(supplier.get());
		assertTestPartitions3(nextScheme);
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

	private void putValue(byte[] bytes) {
		try (KV kv = etcdClient.getKVClient()) {
			try {
				kv.put(bs, ByteSequence.from(bytes)).get();
			} catch (InterruptedException e) {
				throw new AssertionError(e);
			} catch (ExecutionException e) {
				throw new AssertionError(e.getCause());
			}
		}
	}
}
