package io.activej.rpc.client.sender;

import io.activej.async.callback.Callback;
import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import io.activej.test.ExpectedException;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static io.activej.rpc.client.sender.Callbacks.forFuture;
import static io.activej.rpc.client.sender.Callbacks.ignore;
import static io.activej.rpc.client.sender.RpcStrategies.servers;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyFirstValidResultTest {

	private static final String HOST = "localhost";
	private static final Exception NO_VALID_RESULT_EXCEPTION = new ExpectedException("No valid result");

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
	public void itShouldSendRequestToAllAvailableSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection1 = new RpcSenderStub();
		RpcSenderStub connection2 = new RpcSenderStub();
		RpcSenderStub connection3 = new RpcSenderStub();
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.create(servers(address1, address2, address3));
		int callsAmountIterationOne = 10;
		int callsAmountIterationTwo = 25;
		RpcSender senderToAll;

		pool.put(address1, connection1);
		pool.put(address2, connection2);
		pool.put(address3, connection3);
		senderToAll = firstValidResult.createSender(pool);
		for (int i = 0; i < callsAmountIterationOne; i++) {
			senderToAll.sendRequest(new Object(), 50, ignore());
		}
		pool.remove(address1);
		// we should recreate sender after changing in pool
		senderToAll = firstValidResult.createSender(pool);
		for (int i = 0; i < callsAmountIterationTwo; i++) {
			senderToAll.sendRequest(new Object(), 50, ignore());
		}

		assertEquals(callsAmountIterationOne, connection1.getRequests());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection2.getRequests());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection3.getRequests());
	}

	@Test
	public void itShouldCallOnResultWithNullIfAllSendersReturnedNullAndValidatorAndExceptionAreNotSpecified() throws ExecutionException, InterruptedException {
		RpcStrategy strategy1 = new RequestSenderOnResultWithNullRpcStrategy();
		RpcStrategy strategy2 = new RequestSenderOnResultWithNullRpcStrategy();
		RpcStrategy strategy3 = new RequestSenderOnResultWithNullRpcStrategy();
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.create(strategy1, strategy2, strategy3);
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());
		CompletableFuture<Object> future = new CompletableFuture<>();

		sender.sendRequest(new Object(), 50, forFuture(future));

		// despite there are several sender, sendResult should be called only once after all senders returned null

		assertNull(future.get());
	}

	@Test(expected = ExpectedException.class)
	public void itShouldCallOnExceptionIfAllSendersReturnsNullAndValidatorIsDefaultButExceptionIsSpecified() throws Exception {
		// default validator should check whether result is not null
		RpcStrategy strategy1 = new RequestSenderOnResultWithNullRpcStrategy();
		RpcStrategy strategy2 = new RequestSenderOnResultWithNullRpcStrategy();
		RpcStrategy strategy3 = new RequestSenderOnResultWithNullRpcStrategy();
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.builder(strategy1, strategy2, strategy3)
				.withNoValidResultException(NO_VALID_RESULT_EXCEPTION)
				.build();
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());

		CompletableFuture<Object> future = new CompletableFuture<>();
		sender.sendRequest(new Object(), 50, forFuture(future));

		try {
			future.get();
		} catch (ExecutionException e) {
			throw (Exception) e.getCause();
		}
	}

	@Test
	public void itShouldUseCustomValidatorIfItIsSpecified() throws ExecutionException, InterruptedException {
		int invalidKey = 1;
		int validKey = 2;
		RpcStrategy strategy1 = new RequestSenderOnResultWithValueRpcStrategy(invalidKey);
		RpcStrategy strategy2 = new RequestSenderOnResultWithValueRpcStrategy(validKey);
		RpcStrategy strategy3 = new RequestSenderOnResultWithValueRpcStrategy(invalidKey);
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.builder(strategy1, strategy2, strategy3)
				.withResultValidator((Predicate<Integer>) input -> input == validKey)
				.withNoValidResultException(NO_VALID_RESULT_EXCEPTION)
				.build();
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());
		CompletableFuture<Object> future = new CompletableFuture<>();

		sender.sendRequest(new Object(), 50, forFuture(future));

		assertEquals(validKey, future.get());
	}

	@Test(expected = ExecutionException.class)
	public void itShouldCallOnExceptionIfNoSenderReturnsValidResultButExceptionWasSpecified() throws ExecutionException, InterruptedException {
		int invalidKey = 1;
		int validKey = 2;
		RpcStrategy strategy1 = new RequestSenderOnResultWithValueRpcStrategy(invalidKey);
		RpcStrategy strategy2 = new RequestSenderOnResultWithValueRpcStrategy(invalidKey);
		RpcStrategy strategy3 = new RequestSenderOnResultWithValueRpcStrategy(invalidKey);
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.builder(strategy1, strategy2, strategy3)
				.withResultValidator((Predicate<Integer>) input -> input == validKey)
				.withNoValidResultException(NO_VALID_RESULT_EXCEPTION)
				.build();
		RpcSender sender = firstValidResult.createSender(new RpcClientConnectionPoolStub());
		CompletableFuture<Object> future = new CompletableFuture<>();
		sender.sendRequest(new Object(), 50, forFuture(future));
		future.get();
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		// one connection is added
		pool.put(address2, connection);
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.create(servers(address1, address2));
		assertNotNull(firstValidResult.createSender(pool));
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategy firstValidResult = RpcStrategy_FirstValidResult.create(servers(address1, address2, address3));
		assertNull(firstValidResult.createSender(pool));
	}

	private static final class SenderOnResultWithNullCaller implements RpcSender {
		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			cb.accept(null, null);
		}
	}

	static final class SenderOnResultWithValueCaller implements RpcSender {
		private final Object data;

		public SenderOnResultWithValueCaller(Object data) {
			this.data = data;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			cb.accept((O) data, null);
		}
	}

	static final class RequestSenderOnResultWithNullRpcStrategy implements RpcStrategy {
		@Override
		public Set<InetSocketAddress> getAddresses() {
			throw new UnsupportedOperationException();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return new SenderOnResultWithNullCaller();
		}
	}

	static final class RequestSenderOnResultWithValueRpcStrategy implements RpcStrategy {
		private final Object data;

		public RequestSenderOnResultWithValueRpcStrategy(Object data) {
			this.data = data;
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			throw new UnsupportedOperationException();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return new SenderOnResultWithValueCaller(data);
		}
	}
}
