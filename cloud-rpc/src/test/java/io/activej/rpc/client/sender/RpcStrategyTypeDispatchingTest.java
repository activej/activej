package io.activej.rpc.client.sender;

import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcMessageDataStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import io.activej.rpc.client.sender.strategy.RpcStrategy;
import io.activej.rpc.client.sender.strategy.impl.TypeDispatching;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.rpc.client.sender.Callbacks.*;
import static io.activej.rpc.client.sender.strategy.RpcStrategies.server;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyTypeDispatchingTest {

	private static final String HOST = "localhost";

	private InetSocketAddress address1;
	private InetSocketAddress address2;
	private InetSocketAddress address3;
	private InetSocketAddress address4;

	@Before
	public void setUp() {
		address1 = new InetSocketAddress(HOST, getFreePort());
		address2 = new InetSocketAddress(HOST, getFreePort());
		address3 = new InetSocketAddress(HOST, getFreePort());
		address4 = new InetSocketAddress(HOST, getFreePort());
	}

	@Test
	public void itShouldChooseSubStrategyDependingOnRpcMessageDataType() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		RpcStrategy server1 = server(address1);
		RpcStrategy server2 = server(address2);
		RpcStrategy server3 = server(address3);
		RpcStrategy typeDispatchingStrategy = TypeDispatching.builder()
				.with(RpcMessageDataTypeOne.class, server1)
				.with(RpcMessageDataTypeTwo.class, server2)
				.with(RpcMessageDataTypeThree.class, server3)
				.build();
		int dataTypeOneRequests = 1;
		int dataTypeTwoRequests = 2;
		int dataTypeThreeRequests = 5;

		RpcSender sender = typeDispatchingStrategy.createSender(pool);
		for (int i = 0; i < dataTypeOneRequests; i++) {
			sender.sendRequest(new RpcMessageDataTypeOne(), 50, ignore());
		}
		for (int i = 0; i < dataTypeTwoRequests; i++) {
			sender.sendRequest(new RpcMessageDataTypeTwo(), 50, ignore());
		}
		for (int i = 0; i < dataTypeThreeRequests; i++) {
			sender.sendRequest(new RpcMessageDataTypeThree(), 50, ignore());
		}

		assertEquals(dataTypeOneRequests, connection1.getRequests());
		assertEquals(dataTypeTwoRequests, connection2.getRequests());
		assertEquals(dataTypeThreeRequests, connection3.getRequests());
	}

	@Test
	public void itShouldChooseDefaultSubStrategyWhenThereIsNoSpecifiedSubSenderForCurrentDataType() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcSenderStub connection4 = new RpcSenderStub();
		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		pool.put(address4, connection4);
		RpcStrategy server1 = server(address1);
		RpcStrategy server2 = server(address2);
		RpcStrategy server3 = server(address3);
		RpcStrategy defaultServer = server(address4);
		RpcStrategy typeDispatchingStrategy = TypeDispatching.builder()
				.with(RpcMessageDataTypeOne.class, server1)
				.with(RpcMessageDataTypeTwo.class, server2)
				.with(RpcMessageDataTypeThree.class, server3)
				.withDefault(defaultServer)
				.build();

		RpcSender sender = typeDispatchingStrategy.createSender(pool);
		sender.sendRequest(new RpcMessageDataStub(), 50, assertNoCalls());

		assertEquals(0, connection1.getRequests());
		assertEquals(0, connection2.getRequests());
		assertEquals(0, connection3.getRequests());
		assertEquals(1, connection4.getRequests());  // connection of default server

	}

	@Test(expected = ExecutionException.class)
	public void itShouldRaiseExceptionWhenStrategyForDataIsNotSpecifiedAndDefaultSenderIsNull() throws ExecutionException, InterruptedException {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		RpcStrategy server1 = server(address1);
		RpcStrategy server2 = server(address2);
		RpcStrategy server3 = server(address3);
		RpcStrategy typeDispatchingStrategy = TypeDispatching.builder()
				.with(RpcMessageDataTypeOne.class, server1)
				.with(RpcMessageDataTypeTwo.class, server2)
				.with(RpcMessageDataTypeThree.class, server3)
				.build();

		RpcSender sender = typeDispatchingStrategy.createSender(pool);
		// sender is not specified for RpcMessageDataStub, default sender is null
		CompletableFuture<Object> future = new CompletableFuture<>();
		sender.sendRequest(new RpcMessageDataStub(), 50, forFuture(future));

		future.get();
	}

	@Test
	public void itShouldNotBeCreatedWhenAtLeastOneOfCrucialSubStrategyIsNotActive() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy server1 = server(address1);
		RpcStrategy server2 = server(address2);
		RpcStrategy server3 = server(address3);
		RpcStrategy typeDispatchingStrategy = TypeDispatching.builder()
				.with(RpcMessageDataTypeOne.class, server1)
				.with(RpcMessageDataTypeTwo.class, server2)
				.with(RpcMessageDataTypeThree.class, server3)
				.build();

		pool.put(address1, connection1);
		// we don't put connection 2
		pool.put(address3, connection3);

		assertNull(typeDispatchingStrategy.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenDefaultStrategyIsNotActiveAndCrucial() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy server1 = server(address1);
		RpcStrategy server2 = server(address2);
		RpcStrategy server3 = server(address3);
		RpcStrategy defaultServer = server(address4);
		RpcStrategy typeDispatchingStrategy = TypeDispatching.builder()
				.with(RpcMessageDataTypeOne.class, server1)
				.with(RpcMessageDataTypeTwo.class, server2)
				.with(RpcMessageDataTypeThree.class, server3)
				.withDefault(defaultServer)
				.build();

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		// we don't add connection for default server

		assertNull(typeDispatchingStrategy.createSender(pool));
	}

	static class RpcMessageDataTypeOne {

	}

	static class RpcMessageDataTypeTwo {

	}

	static class RpcMessageDataTypeThree {

	}
}
