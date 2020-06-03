package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import io.activej.rpc.hash.HashFunction;
import io.activej.rpc.hash.ShardingFunction;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.Callbacks.ignore;
import static io.activej.rpc.client.sender.RpcStrategies.*;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class RpcStrategiesTest {

	private static final String HOST = "localhost";

	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_4 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_5 = new InetSocketAddress(HOST, getFreePort());

	@Test
	//[START REGION_1]
	public void roundRobinTest() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		RpcSenderStub connection5 = new RpcSenderStub();
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		int iterations = 100;
		RpcStrategy strategy = roundRobin(servers(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));

		RpcSender sender = strategy.createSender(pool);
		for (int i = 0; i < iterations; i++) {
			sender.sendRequest(new Object(), 50, ignore());
		}

		List<RpcSenderStub> connections =
				asList(connection1, connection2, connection3, connection4, connection5);
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
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		// we don't put connection3
		pool.put(ADDRESS_4, connection4);
		int iterations = 20;
		RpcStrategy strategy = roundRobin(
				firstAvailable(servers(ADDRESS_1, ADDRESS_2)),
				firstAvailable(servers(ADDRESS_3, ADDRESS_4)));

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
		pool.put(ADDRESS_1, connection1);
		// we don't put connection2
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		int shardsCount = 2;
		ShardingFunction<Integer> shardingFunction = item -> item % shardsCount;
		RpcStrategy strategy = sharding(shardingFunction,
				firstValidResult(servers(ADDRESS_1, ADDRESS_2)),
				firstValidResult(servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)));

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
		HashFunction<Integer> hashFunction = item -> item;
		RpcStrategy strategy = rendezvousHashing(hashFunction)
				.withShard(1, firstAvailable(servers(ADDRESS_1, ADDRESS_2)))
				.withShard(2, firstAvailable(servers(ADDRESS_3, ADDRESS_4)))
				.withShard(3, server(ADDRESS_5));
		int iterationsPerLoop = 1000;
		RpcSender sender;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		sender = strategy.createSender(pool);
		for (int i = 0; i < iterationsPerLoop; i++) {
			sender.sendRequest(i, 50, ignore());
		}
		//[END REGION_4]
		//[START REGION_5]
		pool.remove(ADDRESS_3);
		pool.remove(ADDRESS_4);
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
		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		pool.put(ADDRESS_4, connection4);
		pool.put(ADDRESS_5, connection5);
		int timeout = 50;
		int iterationsPerDataStub = 25;
		int iterationsPerDataStubWithKey = 35;
		RpcSender sender;
		RpcStrategy strategy = typeDispatching()
				.on(String.class,
						firstValidResult(servers(ADDRESS_1, ADDRESS_2)))
				.onDefault(
						firstAvailable(servers(ADDRESS_3, ADDRESS_4, ADDRESS_5)));

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
