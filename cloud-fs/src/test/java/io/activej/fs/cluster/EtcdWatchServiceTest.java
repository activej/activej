package io.activej.fs.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.time.Stopwatch;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class EtcdWatchServiceTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private EtcdClusterExtension extension;
	private Client etcdClient;
	private EtcdWatchService watchService;
	private ByteSequence bs;

	private static final byte[] INITIAL_VALUE = "initial value".getBytes(UTF_8);
	private static final byte[] VALUE_1 = "value 1".getBytes(UTF_8);
	private static final byte[] VALUE_2 = "value 2".getBytes(UTF_8);
	private static final byte[] VALUE_3 = "value 3".getBytes(UTF_8);

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
		watchService = EtcdWatchService.create(Reactor.getCurrentReactor(), etcdClient, key);
	}

	@After
	public void tearDown() throws Exception {
		etcdClient.close();
		extension.afterEach(null);
	}

	@Test
	public void testEmptyFileChange() {
		AsyncSupplier<byte[]> supplier = watchService.watch();

		byte[] initialValue = await(supplier.get());

		assertArrayEquals(INITIAL_VALUE, initialValue);

		Promise<byte[]> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		putValue(VALUE_1);

		byte[] value1 = await(nextPromise);
		assertArrayEquals(VALUE_1, value1);
	}

	@Test
	public void testNonEmptyFileChange() {
		putValue(VALUE_1);

		AsyncSupplier<byte[]> supplier = watchService.watch();

		byte[] value1 = await(supplier.get());

		assertArrayEquals(VALUE_1, value1);

		Promise<byte[]> nextPromise = supplier.get();

		assertFalse(nextPromise.isComplete());

		putValue(VALUE_2);

		byte[] value2 = await(nextPromise);
		assertArrayEquals(VALUE_2, value2);
	}

	@Test
	public void testFileChangeBetweenGet() {
		AsyncSupplier<byte[]> supplier = watchService.watch();

		byte[] initialValue = await(supplier.get());
		assertArrayEquals(INITIAL_VALUE, initialValue);

		putValue(VALUE_1);

		byte[] value1 = await(supplier.get());
		assertArrayEquals(VALUE_1, value1);
	}

	@Test
	public void testFileChangeMultipleTimesBetweenGet() throws InterruptedException {
		putValue(VALUE_1);

		AsyncSupplier<byte[]> supplier = watchService.watch();

		byte[] value1 = await(supplier.get());
		assertArrayEquals(VALUE_1, value1);

		putValue(VALUE_2);
		putValue(VALUE_3);

		Thread.sleep(100);

		byte[] value3 = await(supplier.get());
		assertArrayEquals(VALUE_3, value3);
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
