package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcMessageDataStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static org.junit.Assert.*;

public class RpcStrategySingleServerTest {

	private static final String HOST = "localhost";

	private InetSocketAddress address;

	@Before
	public void setUp() {
		address = new InetSocketAddress(HOST, 10000);
	}

	@Test
	public void itShouldBeCreatedWhenThereIsConnectionInPool() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		pool.put(address, connection);
		RpcStrategySingleServer strategySingleServer = RpcStrategySingleServer.create(address);

		RpcSender sender = strategySingleServer.createSender(pool);

		assertNotNull(sender);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereIsNoConnectionInPool() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategySingleServer strategySingleServer = RpcStrategySingleServer.create(address);

		RpcSender sender = strategySingleServer.createSender(pool);

		assertNull(sender);
	}

	@Test
	public void itShouldProcessAllCalls() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		pool.put(address, connection);
		RpcStrategySingleServer strategySingleServer = RpcStrategySingleServer.create(address);
		RpcSender sender = strategySingleServer.createSender(pool);
		int calls = 100;
		int timeout = 50;
		RpcMessageDataStub data = new RpcMessageDataStub();

		assert sender != null;
		for (int i = 0; i < calls; i++) {
			sender.sendRequest(data, timeout, assertNoCalls());
		}

		assertEquals(calls, connection.getRequests());
	}
}
