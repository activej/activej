package io.activej.remotefs.cluster;

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.promise.Promises;
import io.activej.remotefs.*;
import io.activej.test.rules.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.promise.TestUtils.await;
import static io.activej.remotefs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static io.activej.test.TestUtils.assertComplete;
import static org.slf4j.event.Level.TRACE;
import static org.slf4j.event.Level.WARN;

@Ignore("takes forever, only for manual testing")
@LoggerConfig(WARN)
@LoggerConfig(packageOf = RemoteFsServer.class, value = TRACE)
public final class TestRepartitionController {
	private static final int CLIENT_SERVER_PAIRS = 10;

	private final Path[] serverStorages = new Path[CLIENT_SERVER_PAIRS];
	private List<RemoteFsServer> servers;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private Path localStorage;
	private FsPartitions partitions;
	private RemoteFsRepartitionController controller;
	private EventloopTaskScheduler scheduler;

	private boolean finished = false;

	@Before
	public void setup() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		Executor executor = Executors.newSingleThreadExecutor();
		servers = new ArrayList<>(CLIENT_SERVER_PAIRS);

		Map<Object, FsClient> clients = new HashMap<>(CLIENT_SERVER_PAIRS);

		Path storage = tmpFolder.newFolder().toPath();
		localStorage = storage.resolve("local");
		Files.createDirectories(localStorage);
		LocalFsClient localFsClient = LocalFsClient.create(eventloop, executor, localStorage);

		Object localPartitionId = "local";
		clients.put(localPartitionId, localFsClient);

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			InetSocketAddress address = new InetSocketAddress("localhost", 5560 + i);

			serverStorages[i] = storage.resolve("storage_" + i);

			Files.createDirectories(serverStorages[i]);

			clients.put("server_" + i, RemoteFsClient.create(eventloop, address));

			RemoteFsServer server = RemoteFsServer.create(eventloop, executor, serverStorages[i]).withListenAddress(address);
			server.listen();
			servers.add(server);
		}

		partitions = FsPartitions.create(eventloop, clients)
				.withServerSelector(RENDEZVOUS_HASH_SHARDER);

		controller = RemoteFsRepartitionController.create(localPartitionId, partitions)
				.withReplicationCount(3);

		scheduler = EventloopTaskScheduler.create(eventloop, partitions::checkDeadPartitions)
				.withInterval(Duration.ofMillis(1000));

		scheduler.start();

		eventloop.delay(200, () -> {
			System.out.println("Closing server_2");
			servers.get(2).close().whenResult(() -> System.out.println("server_2 closed indeed"));
			System.out.println("Closing server_4");
			servers.get(4).close().whenResult(() -> System.out.println("server_4 closed indeed"));
			System.out.println("Closing server_7");
			servers.get(7).close().whenResult(() -> System.out.println("server_7 closed indeed"));
			System.out.println("Closing server_8");
			servers.get(8).close().whenResult(() -> System.out.println("server_8 closed indeed"));
			System.out.println("Closing server_9");
			servers.get(9).close().whenResult(() -> System.out.println("server_9 closed indeed"));
			eventloop.delay(200, () -> {
				try {
					if (finished) {
						return;
					}
					System.out.println("Starting server_7 again");
					servers.get(7).listen();
					System.out.println("Starting server_2 again");
					servers.get(2).listen();
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			});
		});
	}

	private void testN(int n, int minSize, int maxSize) throws IOException {
		long start = System.nanoTime();

		int delta = maxSize - minSize;
		Random rng = new Random(7L);
		for (int i = 0; i < n; i++) {
			byte[] data = new byte[minSize + (delta <= 0 ? 0 : rng.nextInt(delta))];
			rng.nextBytes(data);
			Files.write(localStorage.resolve("file_" + i + ".txt"), data);
		}

		System.out.println("Created local files in " + ((System.nanoTime() - start) / 1e6) + " ms");

		long start2 = System.nanoTime();

		await(controller.repartition()
				.whenComplete(assertComplete($ -> {
					scheduler.stop();
					double ms = (System.nanoTime() - start2) / 1e6;
					System.out.println(String.format("Done repartitioning in %.2f ms", ms));
					Promises.toList(partitions.getAliveClients().values().stream().map(fsClient -> fsClient.list("**").toTry()))
							.map(lss -> lss.stream().mapToLong(ls -> {
								List<FileMetadata> mss = ls.getOrNull();
								return mss == null ? 0 : mss.stream().mapToLong(FileMetadata::getSize).sum();
							}).sum())
							.whenComplete(assertComplete(bytes -> {
								System.out.println(String.format("%d overall bytes", bytes));
								System.out.println(String.format("Average speed was %.2f mbit/second", bytes / (1 << 17) * (1000 / ms)));
								finished = true;
								servers.forEach(AbstractServer::close);
							}));
				})));
	}

	@Test
	public void testTest() throws IOException {
		testN(1, 10 * 1024 * 1024, 50 * 1024 * 1024);
	}

	@Test
	public void testBig50() throws IOException {
		testN(50, 10 * 1024 * 1024, 50 * 1024 * 1024);
	}

	@Test
	public void testMid100() throws IOException {
		testN(100, 10 * 1024, 100 * 1024);
	}

	@Test
	public void testMid1000() throws IOException {
		testN(1000, 10 * 1024, 100 * 1024);
	}

	@Test
	public void test1000() throws IOException {
		testN(1000, 512, 1024);
	}

	@Test
	public void test10000() throws IOException {
		testN(10000, 512, 1024);
	}

	@Test
	public void test100000() throws IOException {
		testN(100000, 512, 1024);
	}

	@Test
	public void test1000000() throws IOException {
		testN(1000000, 512, 1024);
	}
}
