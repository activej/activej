package io.activej.rpc;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;

public final class RpcVoidRequestResponseTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static RpcServer createServer(Eventloop eventloop) {
		return RpcServer.create(eventloop)
				.withMessageTypes(Void.class)
				.withHandler(Void.class, request -> Promise.of(null))
				.withListenPort(port);
	}

	private static class BlockingTestClient implements AutoCloseable {
		private final Eventloop eventloop;
		private final RpcClient rpcClient;

		public BlockingTestClient(Eventloop eventloop) throws Exception {
			this.eventloop = eventloop;
			this.rpcClient = RpcClient.create(eventloop)
					.withMessageTypes(Void.class)
					.withStrategy(server(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port)));

			rpcClient.startFuture().get();
		}

		public void test() throws Exception {
			try {
				rpcClient.getEventloop().submit(
						() -> rpcClient
								.<Void, Void>sendRequest(null, TIMEOUT))
						.get();
			} catch (ExecutionException e) {
				//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
				throw (Exception) e.getCause();
			}
		}

		@Override
		public void close() throws Exception {
			rpcClient.stopFuture().get();
		}
	}

	private static final int TIMEOUT = 1500;
	private static int port;
	private RpcServer server;

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		port = getFreePort();
		server = createServer(eventloop);
		server.listen();
		new Thread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (BlockingTestClient client = new BlockingTestClient(Eventloop.getCurrentEventloop())) {
			for (int i = 0; i < 100; i++) {
				client.test();
			}
		} finally {
			server.closeFuture().get();
		}
	}

	@Test
	public void testAsyncCall() throws Exception {
		int count = 10;
		try (BlockingTestClient client = new BlockingTestClient(Eventloop.getCurrentEventloop())) {
			CountDownLatch latch = new CountDownLatch(count);
			for (int i = 0; i < count; i++) {
				client.eventloop.execute(() -> client.rpcClient.<Void, Void>sendRequest(null, TIMEOUT)
						.whenComplete(latch::countDown)
						.whenComplete(assertCompleteFn(Assert::assertNull)));
			}
			latch.await();
		} finally {
			server.closeFuture().get();
		}
	}

}

