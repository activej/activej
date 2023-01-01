package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.Callbacks.forFuture;
import static io.activej.rpc.client.sender.RpcStrategies.servers;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyShardingTest {

	private static final String HOST = "localhost";

	private InetSocketAddress address1;
	private InetSocketAddress address2;
	private InetSocketAddress address3;

	@Before
	public void setUp() {
		address1 = new InetSocketAddress(HOST, getFreePort());
		address2 = new InetSocketAddress(HOST, getFreePort());
		address3 = new InetSocketAddress(HOST, getFreePort());
	}

	@Test
	public void itShouldSelectSubSenderConsideringHashCodeOfRequestData() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardsAmount = 3;
		RpcStrategy shardingStrategy = RpcStrategies.Sharding.create((Integer item) -> item % shardsAmount,
				servers(address1, address2, address3));
		RpcSender senderSharding;
		int timeout = 50;

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		senderSharding = shardingStrategy.createSender(pool);
		senderSharding.sendRequest(0, timeout, assertNoCalls());
		senderSharding.sendRequest(0, timeout, assertNoCalls());
		senderSharding.sendRequest(1, timeout, assertNoCalls());
		senderSharding.sendRequest(0, timeout, assertNoCalls());
		senderSharding.sendRequest(2, timeout, assertNoCalls());
		senderSharding.sendRequest(0, timeout, assertNoCalls());
		senderSharding.sendRequest(0, timeout, assertNoCalls());
		senderSharding.sendRequest(2, timeout, assertNoCalls());

		assertEquals(5, connection1.getRequests());
		assertEquals(1, connection2.getRequests());
		assertEquals(2, connection3.getRequests());
	}

	@Test
	public void itShouldCallOnExceptionOfCallbackWhenChosenServerIsNotActive() throws InterruptedException {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardsAmount = 3;
		RpcStrategy shardingStrategy = RpcStrategies.Sharding.create((Integer item) -> item % shardsAmount,
				servers(address1, address2, address3));

		// we don't add connection for ADDRESS_1
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		RpcSender sender = shardingStrategy.createSender(pool);

		CompletableFuture<Object> future1 = new CompletableFuture<>();
		CompletableFuture<Object> future2 = new CompletableFuture<>();
		CompletableFuture<Object> future3 = new CompletableFuture<>();

		sender.sendRequest(0, 50, forFuture(future1));
		sender.sendRequest(1, 50, forFuture(future2));
		sender.sendRequest(2, 50, forFuture(future3));

		assertEquals(1, connection2.getRequests());
		assertEquals(1, connection3.getRequests());

		try {
			future1.get();
			fail();
		} catch (ExecutionException e) {
			assertEquals("No senders available", e.getCause().getMessage());
		}
	}

}
