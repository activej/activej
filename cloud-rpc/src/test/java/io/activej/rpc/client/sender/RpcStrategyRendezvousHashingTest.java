package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcMessageDataStubWithKey;
import io.activej.rpc.client.sender.helper.RpcMessageDataStubWithKeyHashFunction;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import io.activej.rpc.hash.HashFunction;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.activej.rpc.client.sender.Callbacks.assertNoCalls;
import static io.activej.rpc.client.sender.RpcStrategies.rendezvousHashing;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

public class RpcStrategyRendezvousHashingTest {

	private static final String HOST = "localhost";

	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, getFreePort());
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, getFreePort());

	@SuppressWarnings("ConstantConditions")
	@Test
	public void itShouldDistributeCallsBetweenActiveSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withShard(shardId1, server1)
				.withShard(shardId2, server2)
				.withShard(shardId3, server3);
		RpcSender sender;
		int callsPerLoop = 10000;
		int timeout = 50;

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);
		sender = rendezvousHashing.createSender(pool);
		for (int i = 0; i < callsPerLoop; i++) {
			sender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, assertNoCalls());
		}
		pool.remove(ADDRESS_1);
		sender = rendezvousHashing.createSender(pool);
		for (int i = 0; i < callsPerLoop; i++) {
			sender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, assertNoCalls());
		}
		pool.remove(ADDRESS_3);
		sender = rendezvousHashing.createSender(pool);
		for (int i = 0; i < callsPerLoop; i++) {
			sender.sendRequest(new RpcMessageDataStubWithKey(i), timeout, assertNoCalls());
		}

		int expectedCallsOfConnection1 = callsPerLoop / 3;
		int expectedCallsOfConnection2 = (callsPerLoop / 3) + (callsPerLoop / 2) + callsPerLoop;
		int expectedCallsOfConnection3 = (callsPerLoop / 3) + (callsPerLoop / 2);
		double delta = callsPerLoop / 30.0;
		assertEquals(expectedCallsOfConnection1, connection1.getRequests(), delta);
		assertEquals(expectedCallsOfConnection2, connection2.getRequests(), delta);
		assertEquals(expectedCallsOfConnection3, connection3.getRequests(), delta);
	}

	@Test
	public void itShouldBeCreatedWhenThereAreAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withShard(shardId1, server1)
				.withShard(shardId2, server2)
				.withShard(shardId3, server3);

		// server3 is active
		pool.put(ADDRESS_3, connection3);

		assertNotNull(rendezvousHashing.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withShard(shardId1, server1)
				.withShard(shardId2, server2)
				.withShard(shardId3, server3);

		// no connections were added to pool, so there are no active servers

		assertNull(rendezvousHashing.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenNoSendersWereAdded() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction);

		assertNull(rendezvousHashing.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withMinActiveShards(4)
				.withShard(shardId1, server1)
				.withShard(shardId2, server2)
				.withShard(shardId3, server3);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		pool.put(ADDRESS_3, connection3);

		assertNotNull(server1.createSender(pool));
		assertNotNull(server2.createSender(pool));
		assertNotNull(server3.createSender(pool));
		assertNull(rendezvousHashing.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		int shardId1 = 1;
		int shardId2 = 2;
		int shardId3 = 3;
		HashFunction<Object> hashFunction = new RpcMessageDataStubWithKeyHashFunction();
		RpcStrategySingleServer server1 = server(ADDRESS_1);
		RpcStrategySingleServer server2 = server(ADDRESS_2);
		RpcStrategySingleServer server3 = server(ADDRESS_3);
		RpcStrategy rendezvousHashing = rendezvousHashing(hashFunction)
				.withMinActiveShards(4)
				.withShard(shardId1, server1)
				.withShard(shardId2, server2)
				.withShard(shardId3, server3);

		pool.put(ADDRESS_1, connection1);
		pool.put(ADDRESS_2, connection2);
		// we don't add connection3

		assertNotNull(server1.createSender(pool));
		assertNotNull(server2.createSender(pool));
		assertNull(server3.createSender(pool));
		assertNull(rendezvousHashing.createSender(pool));
	}

}
