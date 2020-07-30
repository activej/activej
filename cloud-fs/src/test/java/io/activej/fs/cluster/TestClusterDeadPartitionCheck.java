package io.activej.fs.cluster;

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
import io.activej.promise.Promise;
import io.activej.test.rules.ActivePromisesRule;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.fs.util.Utils.initTempDir;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public final class TestClusterDeadPartitionCheck {
	private static final int CLIENT_SERVER_PAIRS = 10;

	private final Path[] serverStorages = new Path[CLIENT_SERVER_PAIRS];
	private List<AbstractServer<?>> servers;
	private List<Eventloop> serverEventloops;

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
										.withListenAddress(address);
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

		Executor executor = Executors.newSingleThreadExecutor();
		servers = new ArrayList<>(CLIENT_SERVER_PAIRS);
		serverEventloops = new ArrayList<>(CLIENT_SERVER_PAIRS);

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
			serverEventloops.add(serverEventloop);

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

		this.partitions = FsPartitions.create(eventloop, partitions);
		this.fs = ClusterActiveFs.create(this.partitions)
				.withReplicationCount(CLIENT_SERVER_PAIRS / 2);
	}

	@After
	public void tearDown() throws Exception {
		for (int i = 0; i < serverEventloops.size(); i++) {
			Eventloop serverEventloop = serverEventloops.get(i);
			AbstractServer<?> server = servers.get(i);
			serverEventloop.submit(server::close).get();
			serverEventloop.keepAlive(false);
			Thread serverEventloopThread = serverEventloop.getEventloopThread();
			if (serverEventloopThread != null) {
				serverEventloopThread.join();
			}
		}
	}

	@Test
	public void testPing() {
		await(fs.ping());
		assertEquals(CLIENT_SERVER_PAIRS, partitions.getAlivePartitions().size());

		setAliveNodes(0, 1, 2, 6, 8, 9);
		await(fs.ping());
		assertEquals(set(0, 1, 2, 6, 8, 9), partitions.getAlivePartitions().keySet());
	}

	@Test
	public void testServersFailWhenUploading() {
		Set<Integer> toBeAlive = set(1, 3, 5);
		Throwable exception = awaitException(fs.upload("test")
				.whenComplete(assertComplete($ -> assertEquals(CLIENT_SERVER_PAIRS, partitions.getAlivePartitions().size())))
				.then(consumer -> {
					setAliveNodes(toBeAlive.toArray(new Integer[0]));
					return ChannelSupplier.of(() -> Promise.of(wrapUtf8("data"))).streamTo(consumer)
							.whenComplete(() -> System.out.println(Eventloop.getCurrentEventloop().getSelector().keys()));
				}));

		assertThat(exception, instanceOf(FsException.class));
		assertThat(exception.getMessage(), containsString("Not enough successes"));
		Set<Object> deadPartitions = partitions.getDeadPartitions().keySet();
		assertEquals(1, deadPartitions.size());
		Integer deadPartition = (Integer) first(deadPartitions);
		assertFalse(toBeAlive.contains(deadPartition));
	}

	private void setAliveNodes(Integer... indexes) {
		Set<Integer> alivePartitions = Arrays.stream(indexes).collect(toSet());
		try {
			for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
				Eventloop eventloop = serverEventloops.get(i);
				AbstractServer<?> server = servers.get(i);
				if (alivePartitions.contains(i)) {
					eventloop.submit(() -> {
						try {
							server.listen();
						} catch (IOException e) {
							throw new AssertionError(e);
						}
					}).get();
				} else {
					eventloop.submit(() -> {
						server.close();
						for (SelectionKey key : eventloop.getSelector().keys()) {
							try {
								key.channel().close();
							} catch (IOException e) {
								throw new AssertionError();
							}
						}
					}).get();
				}
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	private interface ClientServerFactory {
		ActiveFs createClient(Eventloop eventloop, InetSocketAddress address);

		AbstractServer<?> createServer(LocalActiveFs localFs, InetSocketAddress address);
	}
}
