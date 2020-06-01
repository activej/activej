package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcMessageDataStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.RpcStrategies.*;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyRoundRobinTest {

	private static final String HOST = "localhost";

	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_4 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_5 = new InetSocketAddress(HOST, getFreePort());

	@Test
	public void itShouldSendRequestUsingRoundRobinAlgorithm() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy server1 = server(ADDRESS_1);
		RpcStrategy server2 = server(ADDRESS_2);
		RpcStrategy server3 = server(ADDRESS_3);
		RpcStrategy roundRobin = roundRobin(server1, server2, server3);
		RpcSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		int callsAmount = 5;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
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
		RpcStrategy roundRobinStrategy = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcSender senderRoundRobin;
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		int callsAmount = 10;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_4, connection4);
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
		pool.put(ADDRESS_2, connection);
		RpcStrategy roundRobin = RpcStrategyRoundRobin.create(servers(ADDRESS_1, ADDRESS_2));

		assertNotNull(roundRobin.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategy roundRobin = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		assertNull(roundRobin.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy roundRobin = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3))
				.withMinActiveSubStrategies(4);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);

		assertNull(roundRobin.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcStrategy roundRobin = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3))
				.withMinActiveSubStrategies(3);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		// we don't add connection3

		assertNull(roundRobin.createSender(pool));
	}

}
