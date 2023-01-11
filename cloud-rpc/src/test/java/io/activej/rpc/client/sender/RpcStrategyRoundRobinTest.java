package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcMessageDataStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.rpc.client.sender.RpcStrategies.servers;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyRoundRobinTest {

	private static final String HOST = "localhost";

	private InetSocketAddress address1;
	private InetSocketAddress address2;
	private InetSocketAddress address3;
	private InetSocketAddress address4;
	private InetSocketAddress address5;

	@Before
	public void setUp() {
		address1 = new InetSocketAddress(HOST, getFreePort());
		address2 = new InetSocketAddress(HOST, getFreePort());
		address3 = new InetSocketAddress(HOST, getFreePort());
		address4 = new InetSocketAddress(HOST, getFreePort());
		address5 = new InetSocketAddress(HOST, getFreePort());
	}

	@Test
	public void itShouldSendRequestUsingRoundRobinAlgorithm() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy server1 = server(address1);
		RpcStrategy server2 = server(address2);
		RpcStrategy server3 = server(address3);
		RpcStrategy roundRobin = RpcStrategy_RoundRobin.create(server1, server2, server3);
		RpcSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		int callsAmount = 5;

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		senderRoundRobin = roundRobin.createSender(pool);
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, assertNoCalls());
		}

		assertEquals(2, connection1.getRequests());
		assertEquals(2, connection2.getRequests());
		assertEquals(1, connection3.getRequests());
	}

	@Test
	public void itShouldNotSendRequestToNonActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcStrategy roundRobinStrategy = RpcStrategy_RoundRobin.create(servers(address1, address2, address3, address4, address5));
		RpcSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		int callsAmount = 10;

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address4, connection4);
		// we don't add connections for ADDRESS_3 and ADDRESS_5
		senderRoundRobin = roundRobinStrategy.createSender(pool);
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, assertNoCalls());
		}

		assertEquals(4, connection1.getRequests());
		assertEquals(3, connection2.getRequests());
		assertEquals(3, connection4.getRequests());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		// one connection is added
		pool.put(address2, connection);
		RpcStrategy roundRobin = RpcStrategy_RoundRobin.create(servers(address1, address2));

		assertNotNull(roundRobin.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategy roundRobin = RpcStrategy_RoundRobin.create(servers(address1, address2, address3));

		assertNull(roundRobin.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy roundRobin = RpcStrategy_RoundRobin.create(servers(address1, address2, address3))
				.withMinActiveSubStrategies(4);

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);

		assertNull(roundRobin.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcStrategy roundRobin = RpcStrategy_RoundRobin.create(servers(address1, address2, address3))
				.withMinActiveSubStrategies(3);

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		// we don't add connection3

		assertNull(roundRobin.createSender(pool));
	}

}
