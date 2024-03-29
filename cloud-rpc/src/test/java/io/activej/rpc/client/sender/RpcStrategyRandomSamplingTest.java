package io.activej.rpc.client.sender;

import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.client.sender.helper.RpcSenderStub;
import io.activej.rpc.client.sender.strategy.RpcStrategy;
import io.activej.rpc.client.sender.strategy.impl.RandomSampling;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("ConstantConditions")
public class RpcStrategyRandomSamplingTest {
	// we do not care about pool in this test
	private static final RpcClientConnectionPool POOL = null;

	@Test
	public void distributesRequestsProperlyAccordingToWeights() {
		// init subStrategies
		RpcSenderStub sender_1 = new RpcSenderStub();
		StubRpcStrategy strategy_1 = new StubRpcStrategy(sender_1);
		int strategy_1_weight = 2;

		RpcSenderStub sender_2 = new RpcSenderStub();
		StubRpcStrategy strategy_2 = new StubRpcStrategy(sender_2);
		int strategy_2_weight = 3;

		RpcSenderStub sender_3 = new RpcSenderStub();
		StubRpcStrategy strategy_3 = new StubRpcStrategy(sender_3);
		int strategy_3_weight = 7;

		// init RandomSamplingStrategy
		RpcStrategy randomSamplingStrategy = RandomSampling.builder()
			.with(strategy_1_weight, strategy_1)
			.with(strategy_2_weight, strategy_2)
			.with(strategy_3_weight, strategy_3)
			.build();

		// make requests
		RpcSender sender = randomSamplingStrategy.createSender(POOL);
		int totalRequests = 10000;
		for (int i = 0; i < totalRequests; i++) {
			sender.sendRequest(new Object(), 0, (result, e) -> {});
		}

		// check
		assertEquals(totalRequests, sender_1.getRequests() + sender_2.getRequests() + sender_3.getRequests());

		int totalWeight = strategy_1_weight + strategy_2_weight + strategy_3_weight;
		double acceptableError = totalRequests / 50.0;

		double sender_1_expectedRequests = totalRequests * strategy_1_weight / (double) totalWeight;
		assertEquals(sender_1_expectedRequests, sender_1.getRequests(), acceptableError);

		double sender_2_expectedRequests = totalRequests * strategy_2_weight / (double) totalWeight;
		assertEquals(sender_2_expectedRequests, sender_2.getRequests(), acceptableError);

		double sender_3_expectedRequests = totalRequests * strategy_3_weight / (double) totalWeight;
		assertEquals(sender_3_expectedRequests, sender_3.getRequests(), acceptableError);

	}

	@Test
	public void doesNotSendRequestsToStrategiesWithWeightZero() {
		// init subStrategies
		RpcSenderStub sender_1 = new RpcSenderStub();
		StubRpcStrategy strategy_1 = new StubRpcStrategy(sender_1);
		int strategy_1_weight = 2;

		RpcSenderStub sender_2 = new RpcSenderStub();
		StubRpcStrategy strategy_2 = new StubRpcStrategy(sender_2);
		int zero_weight = 0;

		RpcSenderStub sender_3 = new RpcSenderStub();
		StubRpcStrategy strategy_3 = new StubRpcStrategy(sender_3);
		int strategy_3_weight = 8;

		// init RandomSamplingStrategy
		RpcStrategy randomSamplingStrategy = RandomSampling.builder()
			.with(strategy_1_weight, strategy_1)
			.with(zero_weight, strategy_2)
			.with(strategy_3_weight, strategy_3)
			.build();

		// make requests
		RpcSender sender = randomSamplingStrategy.createSender(POOL);
		int totalRequests = 10000;
		for (int i = 0; i < totalRequests; i++) {
			sender.sendRequest(null, 0, (result, e) -> {});
		}

		// check
		assertEquals(totalRequests, sender_1.getRequests() + sender_2.getRequests() + sender_3.getRequests());

		assertEquals(0, sender_2.getRequests());

	}

	private static final class StubRpcStrategy implements RpcStrategy {
		private final RpcSender sender;

		public StubRpcStrategy(RpcSender sender) {
			this.sender = sender;
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return new HashSet<>();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return sender;
		}
	}

}
