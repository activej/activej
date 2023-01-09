package io.activej.fs.cluster;

import io.activej.async.executor.ReactorExecutor;
import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.AsyncFs;
import io.activej.fs.LocalFs;
import io.activej.fs.exception.FsException;
import io.activej.fs.http.FsServlet;
import io.activej.fs.http.HttpFs;
import io.activej.fs.tcp.FsServer;
import io.activej.fs.tcp.RemoteFs;
import io.activej.http.HttpClient;
import io.activej.http.HttpServer;
import io.activej.net.AbstractReactiveServer;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.TestUtils;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
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
import static io.activej.common.Utils.first;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.fs.LocalFs.DEFAULT_TEMP_DIR;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public final class TestClusterDeadPartitionCheck {
	// region configuration
	private static final int CLIENT_SERVER_PAIRS = 10;

	private final Path[] serverStorages = new Path[CLIENT_SERVER_PAIRS];
	private List<AbstractReactiveServer<?>> servers;
	private ExecutorService executor;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Parameter()
	public ClientServerFactory factory;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
				// tcp
				new Object[]{
						new ClientServerFactory() {
							@Override
							public AsyncFs createClient(NioReactor reactor, InetSocketAddress address) {
								return RemoteFs.create(reactor, address);
							}

							@Override
							public AbstractReactiveServer<?> createServer(LocalFs localFs, InetSocketAddress address) {
								return FsServer.create((NioReactor) localFs.getReactor(), localFs)
										.withListenAddress(address);
							}

							@Override
							public void closeServer(AbstractReactiveServer<?> server) {
								server.close();
								Selector selector = server.getReactor().getSelector();
								if (selector == null) return;
								for (SelectionKey key : selector.keys()) {
									Object attachment = key.attachment();
									if (attachment instanceof TcpSocket) {
										((TcpSocket) attachment).close();
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
							public AsyncFs createClient(NioReactor reactor, InetSocketAddress address) {
								return HttpFs.create(reactor, "http://localhost:" + address.getPort(), HttpClient.create(reactor));
							}

							@Override
							public AbstractReactiveServer<?> createServer(LocalFs localFs, InetSocketAddress address) {
								return HttpServer.create((NioReactor) localFs.getReactor(), FsServlet.create(localFs))
										.withReadWriteTimeout(Duration.ZERO, Duration.ZERO)
										.withListenAddress(address);
							}

							@Override
							public void closeServer(AbstractReactiveServer<?> server) {
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
	private ClusterFs fs;

	@Before
	public void setup() throws IOException, ExecutionException, InterruptedException {
		NioReactor reactor = Reactor.getCurrentReactor();

		executor = newSingleThreadExecutor();
		servers = new ArrayList<>(CLIENT_SERVER_PAIRS);

		Map<Object, AsyncFs> partitions = new HashMap<>(CLIENT_SERVER_PAIRS);

		Path storage = tmpFolder.newFolder().toPath();

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			InetSocketAddress address = new InetSocketAddress("localhost", getFreePort());
			partitions.put(i, factory.createClient(reactor, address));

			serverStorages[i] = storage.resolve("storage_" + i);

			Files.createDirectories(serverStorages[i]);

			Eventloop serverEventloop = Eventloop.create().withFatalErrorHandler(rethrow());
			serverEventloop.keepAlive(true);

			LocalFs localFs = LocalFs.create(serverEventloop, executor, serverStorages[i]);
			AbstractReactiveServer<?> server = factory.createServer(localFs, address);
			CompletableFuture<Void> startFuture = serverEventloop.submit(() -> {
				try {
					server.listen();
					return localFs.start();
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			});
			servers.add(server);
			new Thread(serverEventloop).start();
			startFuture.get();
		}

		this.partitions = FsPartitions.create(reactor, AsyncDiscoveryService.constant(partitions))
				.withServerSelector((fileName, shards) -> shards.stream().sorted().collect(toList()));
		await(this.partitions.start());
		this.fs = ClusterFs.create(reactor, this.partitions)
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
		assertEquals(Set.of(0, 1, 2, 6, 8, 9), partitions.getAlivePartitions().keySet());
	}

	@Test
	public void testServersFailOnStreamingUpload() {
		Set<Integer> toBeAlive = Set.of(1, 3);
		String filename = "test";
		Exception exception = awaitException(fs.upload(filename)
				.whenComplete(TestUtils.assertCompleteFn($ -> assertEquals(CLIENT_SERVER_PAIRS, partitions.getAlivePartitions().size())))
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
											.toList();

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
				AbstractReactiveServer<?> server = servers.get(i);
				ReactorExecutor reactorExecutor = server.getReactor();

				int finalI = i;
				reactorExecutor.submit(() -> {
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
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		} catch (ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	private static Set<Path> listAllFiles(Path dir) {
		Set<Path> files = new HashSet<>();
		try {
			Files.walkFileTree(dir, new SimpleFileVisitor<>() {
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
			for (AbstractReactiveServer<?> server : servers) {
				Eventloop serverEventloop = (Eventloop) server.getReactor();
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
			//noinspection ResultOfMethodCallIgnored
			executor.awaitTermination(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		} catch (ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	private interface ClientServerFactory {
		AsyncFs createClient(NioReactor reactor, InetSocketAddress address);

		AbstractReactiveServer<?> createServer(LocalFs localFs, InetSocketAddress address);

		void closeServer(AbstractReactiveServer<?> server) throws IOException;
	}
}
