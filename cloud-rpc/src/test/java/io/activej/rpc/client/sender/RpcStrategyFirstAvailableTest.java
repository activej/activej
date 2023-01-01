package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.RpcStrategies.servers;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

public class RpcStrategyFirstAvailableTest {

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

	@SuppressWarnings("ConstantConditions")
	@Test
	public void itShouldSendRequestToFirstAvailableSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
        RpcStrategy firstAvailableStrategy = RpcStrategies.FirstAvailable.create(servers(address1, address2, address3));
		RpcSender sender;
		int callsToSender1 = 10;
		int callsToSender2 = 25;
		int callsToSender3 = 32;

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		sender = firstAvailableStrategy.createSender(pool);
		for (int i = 0; i < callsToSender1; i++) {
			sender.sendRequest(new Object(), 50, assertNoCalls());
		}
		pool.remove(address1);
		// we should recreate sender after changing in pool
		sender = firstAvailableStrategy.createSender(pool);
		for (int i = 0; i < callsToSender2; i++) {
			sender.sendRequest(new Object(), 50, assertNoCalls());
		}
		pool.remove(address2);
		// we should recreate sender after changing in pool
		sender = firstAvailableStrategy.createSender(pool);
		for (int i = 0; i < callsToSender3; i++) {
			sender.sendRequest(new Object(), 50, assertNoCalls());
		}

		assertEquals(callsToSender1, connection1.getRequests());
		assertEquals(callsToSender2, connection2.getRequests());
		assertEquals(callsToSender3, connection3.getRequests());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		// one connection is added
		pool.put(address2, connection);
        RpcStrategy firstAvailableStrategy =
                RpcStrategies.FirstAvailable.create(servers(address1, address2));

		assertNotNull(firstAvailableStrategy.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
        RpcStrategy firstAvailableStrategy = RpcStrategies.FirstAvailable.create(servers(address1, address2, address3));

		assertNull(firstAvailableStrategy.createSender(pool));
	}
}
