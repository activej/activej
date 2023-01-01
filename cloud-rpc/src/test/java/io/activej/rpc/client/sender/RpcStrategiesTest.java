package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.Callbacks.ignore;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.rpc.client.sender.RpcStrategies.servers;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class RpcStrategiesTest {

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
	//[START REGION_1]
	public void roundRobinTest() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		pool.put(address4, connection4);
		pool.put(address5, connection5);
		int iterations = 100;
		RpcStrategy strategy = RpcStrategies.RoundRobin.create(servers(address1, address2, address3, address4, address5));

		RpcSender sender = strategy.createSender(pool);
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(new Object(), 50, ignore());
		}

		List<RpcSenderStub> connections =
				List.of(connection1, connection2, connection3, connection4, connection5);
		for (int i = 0; i < 5; i++) {
			assertEquals(iterations / 5, connections.get(i).getRequests());
		}
	}
	//[END REGION_1]

	@Test
	//[START REGION_2]
	public void roundRobinAndFirstAvailableTest() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		pool.put(address1, connection1);
		pool.put(address2, connection2);
		// we don't put connection3
		pool.put(address4, connection4);
		int iterations = 20;
		RpcStrategy strategy = RpcStrategies.RoundRobin.create(
				RpcStrategies.FirstAvailable.create(servers(address1, address2)),
				RpcStrategies.FirstAvailable.create(servers(address3, address4)));

		RpcSender sender = strategy.createSender(pool);
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(new Object(), 50, assertNoCalls());
		}

		assertEquals(iterations / 2, connection1.getRequests());
		assertEquals(0, connection2.getRequests());
		assertEquals(0, connection3.getRequests());
		assertEquals(iterations / 2, connection4.getRequests());
	}
	//[END REGION_2]

	@Test
	//[START REGION_3]
	public void shardingAndFirstValidTest() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(address1, connection1);
		// we don't put connection2
		pool.put(address3, connection3);
		pool.put(address4, connection4);
		pool.put(address5, connection5);
		int shardsCount = 2;
		RpcStrategy strategy = RpcStrategies.Sharding.create((Integer item) -> item % shardsCount,
				RpcStrategies.FirstValidResult.create(servers(address1, address2)),
				RpcStrategies.FirstValidResult.create(servers(address3, address4, address5)));

		RpcSender sender = strategy.createSender(pool);
		sender.sendRequest(0, 50, assertNoCalls());
		sender.sendRequest(0, 50, assertNoCalls());
		sender.sendRequest(1, 50, assertNoCalls());
		sender.sendRequest(1, 50, assertNoCalls());
		sender.sendRequest(0, 50, assertNoCalls());
		sender.sendRequest(0, 50, assertNoCalls());
		sender.sendRequest(1, 50, assertNoCalls());

		assertEquals(4, connection1.getRequests());
		assertEquals(0, connection2.getRequests());
		assertEquals(3, connection3.getRequests());
		assertEquals(3, connection4.getRequests());
		assertEquals(3, connection5.getRequests());
	}
	//[END REGION_3]

	@Test
	//[START REGION_4]
	public void rendezvousHashingTest() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		RpcStrategy strategy = RpcStrategies.RendezvousHashing.create((Integer item) -> item)
				.withShard(1, RpcStrategies.FirstAvailable.create(servers(address1, address2)))
				.withShard(2, RpcStrategies.FirstAvailable.create(servers(address3, address4)))
				.withShard(3, server(address5));
		int iterationsPerLoop = 1000;
		RpcSender sender;

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		pool.put(address4, connection4);
		pool.put(address5, connection5);
		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerLoop; i++) {
			sender.sendRequest(i, 50, ignore());
		}
		//[END REGION_4]
		//[START REGION_5]
		pool.remove(address3);
		pool.remove(address4);
		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerLoop; i++) {
			sender.sendRequest(i, 50, ignore());
		}

		double acceptableError = iterationsPerLoop / 10.0;
		assertEquals(iterationsPerLoop / 3.0 + iterationsPerLoop / 2.0, connection1.getRequests(), acceptableError);
		assertEquals(0, connection2.getRequests());
		assertEquals(iterationsPerLoop / 3.0, connection3.getRequests(), acceptableError);
		assertEquals(0, connection4.getRequests());
		assertEquals(iterationsPerLoop / 3.0 + iterationsPerLoop / 2.0, connection5.getRequests(), acceptableError);
	}
	//[END REGION_5]

	@Test
	//[START REGION_6]
	public void typeDispatchTest() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		pool.put(address4, connection4);
		pool.put(address5, connection5);
		int timeout = 50;
		int iterationsPerDataStub = 25;
		int iterationsPerDataStubWithKey = 35;
		RpcSender sender;
		RpcStrategy strategy = RpcStrategies.TypeDispatching.create()
				.on(String.class,
						RpcStrategies.FirstValidResult.create(servers(address1, address2)))
				.onDefault(
						RpcStrategies.FirstAvailable.create(servers(address3, address4, address5)));

		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerDataStub; i++) {
			sender.sendRequest(new Object(), timeout, assertNoCalls());
		}
		for (int i = 0; i < iterationsPerDataStubWithKey; i++) {
			sender.sendRequest("request", timeout, assertNoCalls());
		}

		assertEquals(iterationsPerDataStubWithKey, connection1.getRequests());
		assertEquals(iterationsPerDataStubWithKey, connection2.getRequests());
		assertEquals(iterationsPerDataStub, connection3.getRequests());
		assertEquals(0, connection4.getRequests());
		assertEquals(0, connection5.getRequests());
	}
	//[END REGION_6]
}
