package io.activej.fs.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.time.Stopwatch;
import io.activej.fs.ActiveFs;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.EventloopRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;
import org.junit.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
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

	private static final byte[] INITIAL_VALUE = "[]".getBytes(UTF_8);
	private static final byte[] PARTITIONS_1 = "[\"partition1\"]".getBytes(UTF_8);
	private static final byte[] PARTITIONS_2 = "[\"partition1\", \"partition2\"]".getBytes(UTF_8);

	private static final Map<Object, ActiveFs> PARTITIONS = new HashMap<>();

	@BeforeClass
	public static void beforeClass() {
		NioReactor reactor = Reactor.getCurrentReactor();
		PARTITIONS.put("partition1", RemoteActiveFs.create(reactor, new InetSocketAddress(0)));
		PARTITIONS.put("partition2", RemoteActiveFs.create(reactor, new InetSocketAddress(0)));
	}

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
		putValue(INITIAL_VALUE);
		EtcdWatchService watchService = EtcdWatchService.create(Reactor.getCurrentReactor(), etcdClient, key);
		this.discoveryService = EtcdDiscoveryService.create(watchService, PARTITIONS::get);
	}

	@After
	public void tearDown() throws Exception {
		etcdClient.close();
		extension.afterEach(null);
	}

	@Test
	public void testEmptyFileChange() {
		AsyncSupplier<Map<Object, ActiveFs>> supplier = discoveryService.discover();

		Map<Object, ActiveFs> initialPartitions = await(supplier.get());

		assertTrue(initialPartitions.isEmpty());

		Promise<Map<Object, ActiveFs>> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		putValue(PARTITIONS_1);

		Map<Object, ActiveFs> partitions1 = await(nextPromise);
		assertEquals(Set.of("partition1"), partitions1.keySet());
		assertSame(partitions1.get("partition1"), PARTITIONS.get("partition1"));
	}

	@Test
	public void testNonEmptyFileChange() {
		putValue(PARTITIONS_1);

		AsyncSupplier<Map<Object, ActiveFs>> supplier = discoveryService.discover();

		Map<Object, ActiveFs> partitions1 = await(supplier.get());
		assertEquals(Set.of("partition1"), partitions1.keySet());
		assertSame(partitions1.get("partition1"), PARTITIONS.get("partition1"));

		Promise<Map<Object, ActiveFs>> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		putValue(PARTITIONS_2);

		Map<Object, ActiveFs> partitions2 = await(nextPromise);
		assertEquals(Set.of("partition1", "partition2"), partitions2.keySet());
		assertSame(partitions2.get("partition1"), PARTITIONS.get("partition1"));
		assertSame(partitions2.get("partition2"), PARTITIONS.get("partition2"));
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
