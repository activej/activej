package io.activej.rpc.client;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.ref.RefBoolean;
import io.activej.common.ref.RefInt;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategy_FirstValidResult;
import io.activej.rpc.client.sender.RpcStrategy_RoundRobin;
import io.activej.rpc.protocol.RpcException;
import io.activej.rpc.server.RpcServer;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.rpc.client.sender.RpcStrategies.servers;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

public final class RpcClient_Reactive_Test {
	private static final int NUMBER_OF_WORKING_SERVERS = 6;
	private static final int NUMBER_OF_FAILING_SERVERS = 4;

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public static final Request REQUEST = new Request();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private Map<Integer, RpcServer> servers;
	private Eventloop clientEventloop;
	private RpcClient_Reactive rpcClient;

	@Before
	public void setUp() throws Exception {
		servers = new HashMap<>();

		CountDownLatch latch = new CountDownLatch(NUMBER_OF_WORKING_SERVERS);
		for (int i = 0; i < NUMBER_OF_WORKING_SERVERS + NUMBER_OF_FAILING_SERVERS; i++) {
			Eventloop serverEventloop = Eventloop.create()
					.withFatalErrorHandler(rethrow());
			serverEventloop.keepAlive(true);
			new Thread(serverEventloop).start();

			int finalI = i;
			RpcServer rpcServer = RpcServer.create(serverEventloop)
					.withMessageTypes(Request.class, Integer.class)
					.withHandler(Request.class, $ -> Promise.of(finalI))
					.withListenPort(getFreePort());

			if (i < NUMBER_OF_WORKING_SERVERS) {
				serverEventloop.submit(() -> {
					try {
						rpcServer.listen();
						latch.countDown();
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				});
			}

			servers.put(i, rpcServer);
		}
		latch.await();

		clientEventloop = Eventloop.create()
				.withFatalErrorHandler(rethrow());
		clientEventloop.keepAlive(true);
		new Thread(clientEventloop).start();
	}

	@After
	public void tearDown() throws InterruptedException, ExecutionException {
		CompletableFuture<Void> clientStopFuture = clientEventloop.submit(rpcClient::stop);
		clientEventloop.keepAlive(false);

		for (RpcServer server : servers.values()) {
			Eventloop serverEventloop = (Eventloop) server.getReactor();
			CompletableFuture<?> serverCloseFuture = serverEventloop.submit(server::close);
			serverEventloop.keepAlive(false);
			serverCloseFuture.get();
			Thread serverEventloopThread = serverEventloop.getEventloopThread();
			if (serverEventloopThread != null) {
				serverEventloopThread.join();
			}
		}

		clientStopFuture.get();
		Thread clientEventloopThread = clientEventloop.getEventloopThread();
		if (clientEventloopThread != null) {
			clientEventloopThread.join();
		}
	}

	@Test
	public void testSendRequest() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 2, 4)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));
		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(2, response2);
		assertEquals(4, response3);
		assertEquals(0, response4);
	}

	@Test
	public void testChangeStrategyNewAddresses() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(1, response2);

		await(() -> rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(2, 3)), false));

		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(2, response3);
		assertEquals(3, response4);
	}

	@Test
	public void testChangeStrategyAdditionalAddresses() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(1, response2);

		await(() -> rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(0, 1, 2, 3)), false));

		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));
		int response5 = await(() -> rpcClient.sendRequest(REQUEST));
		int response6 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response3);
		assertEquals(1, response4);
		assertEquals(2, response5);
		assertEquals(3, response6);
	}

	@Test
	public void testChangeStrategyMixedAddresses() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(1, response2);

		await(() -> rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(0, 2, 3)), false));

		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));
		int response5 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response3);
		assertEquals(2, response4);
		assertEquals(3, response5);
	}

	@Test
	public void testChangeStrategyOngoingRequests() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		await(() -> {
			RefBoolean changed = new RefBoolean(false);
			rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(0, 3)), false)
					.whenResult(() -> changed.set(true));

			RefInt counter = new RefInt(100);
			return sendAndExpect(0, 1, changed::get)
					.then(() -> sendAndExpect(0, 3, () -> counter.dec() == 0));
		});
	}

	@Test
	public void testChangeStrategySameAddresses() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		await(() -> {
			Promise<Void> changePromiseSame = rpcClient.changeStrategy(RpcStrategy_FirstValidResult.create(getAddresses(0, 1)), false);
			assertTrue(changePromiseSame.isResult());

			Promise<Void> changePromiseDifferent = rpcClient.changeStrategy(RpcStrategy_FirstValidResult.create(getAddresses(0, 1, 2)), false);
			assertFalse(changePromiseDifferent.isComplete());

			return changePromiseDifferent;
		});
	}

	@Test
	public void testChangeStrategyMultipleTimes() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		await(() -> {
			Promise<Void> first = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(0, 1, 2)), false);
			Promise<Void> second = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(0, 1, 2, 3)), false);
			Promise<Void> third = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(1, 3, 4, 5)), false);

			assertTrue(first.isException());
			assertTrue(first.getException() instanceof RpcException);
			assertEquals("Could not change strategy", first.getException().getMessage());

			assertTrue(second.isException());
			assertTrue(second.getException() instanceof RpcException);
			assertEquals("Could not change strategy", second.getException().getMessage());

			return third;
		});

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));
		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(1, response1);
		assertEquals(3, response2);
		assertEquals(4, response3);
		assertEquals(5, response4);
	}

	@Test
	public void testChangeStrategyClientStop() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		Exception exception = awaitException(() -> {
			Promise<Void> changeStrategyPromise = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(1, 2)), false);
			rpcClient.stop();
			return changeStrategyPromise;
		});

		assertTrue(exception instanceof RpcException);
		assertEquals("Client is stopped", exception.getMessage());
	}

	@Test
	public void testChangeStrategyFailingServers() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(1, response2);

		Exception exception = awaitException(() -> rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(6, 7)), false));

		assertTrue(exception instanceof RpcException);
		assertEquals("Could not establish connection", exception.getMessage());

		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response3);
		assertEquals(1, response4);
	}

	@Test
	public void testMultipleChangeStrategyFailingServers() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(1, response2);

		await(() -> rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(6, 7)), false)
				.then(($, e) -> {
					assertTrue(e instanceof RpcException);
					assertEquals("Could not establish connection", e.getMessage());

					return rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(8, 9)), false);
				})
				.then(($, e) -> {
					assertTrue(e instanceof RpcException);
					assertEquals("Could not establish connection", e.getMessage());

					return rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(3, 4)), false);
				}));


		int response3 = await(() -> rpcClient.sendRequest(REQUEST));
		int response4 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(3, response3);
		assertEquals(4, response4);
	}

	@Test
	public void testChangeStrategyRetry() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		int response1 = await(() -> rpcClient.sendRequest(REQUEST));
		int response2 = await(() -> rpcClient.sendRequest(REQUEST));

		assertEquals(0, response1);
		assertEquals(1, response2);

		await(() -> {
			Promise<Void> changeStrategy = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(6, 7)), true);

			RefBoolean serversListen = new RefBoolean(false);
			Reactor.getCurrentReactor().delay(Duration.ofMillis(100), () -> {
				assertFalse(changeStrategy.isComplete());

				listen(6);
				listen(7);
				serversListen.set(true);
			});

			return changeStrategy
					.whenResult(() -> assertTrue(serversListen.get()));
		});

		boolean server6Response = false;
		boolean server7Response = false;
		while (!(server6Response && server7Response)) {
			int serverIdx = await(() -> rpcClient.sendRequest(REQUEST));

			if (serverIdx == 6) {
				server6Response = true;
			} else if (serverIdx == 7) {
				server7Response = true;
			} else {
				throw new AssertionError();
			}
		}
	}

	@Test
	public void testChangeStrategyRetryClientStop() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		Exception exception = awaitException(() -> {
			Promise<Void> changeStrategy = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(6, 7)), true);

			RefBoolean clientStopped = new RefBoolean(false);
			Reactor.getCurrentReactor().delay(Duration.ofMillis(100), () -> {
				assertFalse(changeStrategy.isComplete());

				rpcClient.stop();
				clientStopped.set(true);
			});

			return changeStrategy
					.whenException(() -> assertTrue(clientStopped.get()));
		});

		assertTrue(exception instanceof RpcException);
		assertEquals("Client is stopped", exception.getMessage());
	}

	@Test
	public void testChangeStrategyRetryChangedStrategy() {
		initRpcClient(RpcStrategy_RoundRobin.create(getAddresses(0, 1)));

		Exception exception = awaitException(() -> {
			Promise<Void> changeStrategy = rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(6, 7)), true);

			RefBoolean strategyChanged = new RefBoolean(false);
			Reactor.getCurrentReactor().delay(Duration.ofMillis(100), () -> {
				assertFalse(changeStrategy.isComplete());

				strategyChanged.set(true);
				rpcClient.changeStrategy(RpcStrategy_RoundRobin.create(getAddresses(1, 2, 3, 4)), false);
			});

			return changeStrategy
					.whenException(() -> assertTrue(strategyChanged.get()));
		});

		assertTrue(exception instanceof RpcException);
		assertEquals("Could not change strategy", exception.getMessage());
	}

	private Promise<Void> sendAndExpect(int id1, int id2, Supplier<Boolean> stopCondition) {
		return Promises.all(
						rpcClient.sendRequest(REQUEST)
								.whenComplete(assertCompleteFn(response -> assertEquals(id1, response))),
						rpcClient.sendRequest(REQUEST)
								.whenComplete(assertCompleteFn(response -> assertEquals(id2, response)))
				)
				.then(() -> {
					if (stopCondition.get()) return Promise.complete();
					return sendAndExpect(id1, id2, stopCondition);
				});
	}

	private void initRpcClient(RpcStrategy strategy) {
		this.rpcClient = RpcClient_Reactive.create(clientEventloop)
				.withMessageTypes(Request.class, Integer.class)
				.withStrategy(strategy);
		await(rpcClient::start);
	}

	private List<RpcStrategy> getAddresses(int... indexes) {
		return servers(Arrays.stream(indexes)
				.mapToObj(servers::get)
				.map(server -> server.getListenAddresses().get(0))
				.collect(Collectors.toList()));
	}

	private void listen(int index) {
		RpcServer server = servers.get(index);
		server.getReactor().submit(() -> {
			try {
				server.listen();
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		});
	}

	private <T> T await(AsyncSupplier<T> supplier) {
		CompletableFuture<T> result = clientEventloop.submit(supplier::get);
		try {
			return result.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T, E extends Exception> E awaitException(AsyncSupplier<T> supplier) {
		CompletableFuture<?> result = clientEventloop.submit(supplier::get);
		try {
			result.get();
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		} catch (ExecutionException e) {
			return (E) e.getCause();
		} catch (Exception e) {
			return (E) e;
		}
		throw new AssertionError("Promise did not fail");
	}

	public static final class Request {
	}

}
