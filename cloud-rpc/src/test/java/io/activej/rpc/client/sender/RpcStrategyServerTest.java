package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcMessageDataStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import io.activej.rpc.client.sender.strategy.RpcStrategy;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.strategy.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

public class RpcStrategyServerTest {

	private static final String HOST = "localhost";

	private InetSocketAddress address;

	@Before
	public void setUp() {
		address = new InetSocketAddress(HOST, getFreePort());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsConnectionInPool() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		pool.put(address, connection);
		RpcStrategy strategySingleServer = server(address);

		RpcSender sender = strategySingleServer.createSender(pool);

		assertNotNull(sender);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereIsNoConnectionInPool() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategy strategySingleServer = server(address);

		RpcSender sender = strategySingleServer.createSender(pool);

		assertNull(sender);
	}

	@Test
	public void itShouldProcessAllCalls() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		pool.put(address, connection);
		RpcStrategy strategySingleServer = server(address);
		RpcSender sender = strategySingleServer.createSender(pool);
		int calls = 100;
		int timeout = 50;
		RpcMessageDataStub data = new RpcMessageDataStub();

		assertNotNull(sender);
		for (int i = 0; i < calls; i++) {
			sender.sendRequest(data, timeout, assertNoCalls());
		}

		assertEquals(calls, connection.getRequests());
	}
}
