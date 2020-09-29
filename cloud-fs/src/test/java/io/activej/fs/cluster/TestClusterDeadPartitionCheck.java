package io.activej.fs.cluster;

import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.fs.exception.FsException;
import io.activej.fs.http.ActiveFsServlet;
import io.activej.fs.http.HttpActiveFs;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.http.AsyncHttpClient;
import io.activej.http.AsyncHttpServer;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import io.activej.test.rules.LoggingRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.fs.LocalActiveFs.DEFAULT_TEMP_DIR;
import static io.activej.fs.Utils.initTempDir;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public final class TestClusterDeadPartitionCheck {
	// region configuration
	private static final int CLIENT_SERVER_PAIRS = 10;

	private final Path[] serverStorages = new Path[CLIENT_SERVER_PAIRS];
	private List<AbstractServer<?>> servers;
	private ExecutorService executor;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Parameter()
	public ClientServerFactory factory;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				// tcp
				new Object[]{
						new ClientServerFactory() {
							@Override
							public ActiveFs createClient(Eventloop eventloop, InetSocketAddress address) {
								return RemoteActiveFs.create(eventloop, address);
							}

							@Override
							public AbstractServer<?> createServer(LocalActiveFs localFs, InetSocketAddress address) {
								return ActiveFsServer.create(localFs.getEventloop(), localFs)
										.withListenAddress(address);
							}

							@Override
							public void closeServer(AbstractServer<?> server) {
								server.close();
								Selector selector = server.getEventloop().getSelector();
								if (selector == null) return;
								for (SelectionKey key : selector.keys()) {
									Object attachment = key.attachment();
									if (attachment instanceof AsyncTcpSocketNio) {
										((AsyncTcpSocketNio) attachment).close();
									}
								}
							}

							@Override
							public String toString() {
								return "TCP";
							}
						}
				},

				// http
				new Object[]{
						new ClientServerFactory() {
							@Override
							public ActiveFs createClient(Eventloop eventloop, InetSocketAddress address) {
								return HttpActiveFs.create("http://localhost:" + address.getPort(), AsyncHttpClient.create(eventloop));
							}

							@Override
							public AbstractServer<?> createServer(LocalActiveFs localFs, InetSocketAddress address) {
								return AsyncHttpServer.create(localFs.getEventloop(), ActiveFsServlet.create(localFs))
										.withReadWriteTimeout(Duration.ZERO, Duration.ZERO)
										.withListenAddress(address);
							}

							@Override
							public void closeServer(AbstractServer<?> server) {
								server.close();
							}

							@Override
							public String toString() {
								return "HTTP";
							}
						}
				}
		);
	}

	private FsPartitions partitions;
	private ClusterActiveFs fs;

	@Before
	public void setup() throws IOException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		executor = newSingleThreadExecutor();
		servers = new ArrayList<>(CLIENT_SERVER_PAIRS);

		Map<Object, ActiveFs> partitions = new HashMap<>(CLIENT_SERVER_PAIRS);

		Path storage = tmpFolder.newFolder().toPath();

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			InetSocketAddress address = new InetSocketAddress("localhost", getFreePort());
			partitions.put(i, factory.createClient(eventloop, address));

			serverStorages[i] = storage.resolve("storage_" + i);

			Files.createDirectories(serverStorages[i]);

			initTempDir(serverStorages[i]);

			Eventloop serverEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
			serverEventloop.keepAlive(true);

			LocalActiveFs localFs = LocalActiveFs.create(serverEventloop, executor, serverStorages[i]);
			AbstractServer<?> server = factory.createServer(localFs, address);
			CompletableFuture<Void> listenFuture = serverEventloop.submit(() -> {
				try {
					server.listen();
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			});
			servers.add(server);
			new Thread(serverEventloop).start();
			listenFuture.get();
		}

		this.partitions = FsPartitions.create(eventloop, partitions)
				.withServerSelector((fileName, shards) -> shards.stream().sorted().collect(toList()));
		this.fs = ClusterActiveFs.create(this.partitions)
				.withReplicationCount(CLIENT_SERVER_PAIRS / 2);
	}

	@After
	public void tearDown() {
		waitForServersToStop();
	}
	// endregion

	@Test
	public void testPing() {
		await(fs.ping());
		assertEquals(CLIENT_SERVER_PAIRS, partitions.getAlivePartitions().size());

		setAliveNodes(0, 1, 2, 6, 8, 9);
		await(fs.ping());
		assertEquals(set(0, 1, 2, 6, 8, 9), partitions.getAlivePartitions().keySet());
	}

	@Test
	public void testServersFailOnStreamingUpload() {
		Set<Integer> toBeAlive = set(1, 3);
		String filename = "test";
		Throwable exception = awaitException(fs.upload(filename)
				.whenComplete(assertComplete($ -> assertEquals(CLIENT_SERVER_PAIRS, partitions.getAlivePartitions().size())))
				.then(consumer -> {
					RefInt dataBeforeShutdown = new RefInt(100);
					return ChannelSupplier.of(() -> Promise.of(wrapUtf8("data")))
							.peek($ -> {
								if (dataBeforeShutdown.dec() == 0) {
									List<Path> allFiles = Arrays.stream(serverStorages)
											.flatMap(path -> {
												Set<Path> files = listAllFiles(path);
												assertTrue(files.size() <= 1);
												return files.stream();
											})
											.collect(toList());

									// temporary files are created
									assertEquals(fs.getUploadTargetsMax(), allFiles.size());

									// no real files are created yet
									assertTrue(allFiles.stream().allMatch(path -> path.toString().contains(DEFAULT_TEMP_DIR)));

									setAliveNodes(toBeAlive.toArray(new Integer[0]));
								}
							})
							.streamTo(consumer);
				}));

		assertThat(exception, instanceOf(FsException.class));
		assertThat(exception.getMessage(), containsString("Not enough successes"));
		Set<Object> deadPartitions = partitions.getDeadPartitions().keySet();

		// only first failed partition is marked dead
		assertEquals(1, deadPartitions.size());
		Integer deadPartition = (Integer) first(deadPartitions);
		assertFalse(toBeAlive.contains(deadPartition));

		waitForServersToStop();

		// No new files created on alive partitions
		for (Integer id : toBeAlive) {
			assertTrue(listAllFiles(serverStorages[id]).isEmpty());
		}
	}

	private void setAliveNodes(Integer... indexes) {
		Set<Integer> alivePartitions = Arrays.stream(indexes).collect(toSet());
		try {
			for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
				AbstractServer<?> server = servers.get(i);
				Eventloop eventloop = server.getEventloop();

				int finalI = i;
				eventloop.submit(() -> {
					try {
						if (alivePartitions.contains(finalI)) {
							server.listen();
						} else {
							factory.closeServer(server);
						}
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				}).get();
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	private static Set<Path> listAllFiles(Path dir) {
		Set<Path> files = new HashSet<>();
		try {
			Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
					files.add(file);
					return CONTINUE;
				}
			});
		} catch (IOException e) {
			throw new AssertionError(e);
		}
		return files;
	}

	private void waitForServersToStop() {
		try {
			for (AbstractServer<?> server : servers) {
				Eventloop serverEventloop = server.getEventloop();
				if (server.isRunning()) {
					serverEventloop.submit(server::close).get();
				}
				serverEventloop.keepAlive(false);
				Thread serverEventloopThread = serverEventloop.getEventloopThread();
				if (serverEventloopThread != null) {
					serverEventloopThread.join();
				}
			}
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	private interface ClientServerFactory {
		ActiveFs createClient(Eventloop eventloop, InetSocketAddress address);

		AbstractServer<?> createServer(LocalActiveFs localFs, InetSocketAddress address);

		void closeServer(AbstractServer<?> server) throws IOException;
	}
}
