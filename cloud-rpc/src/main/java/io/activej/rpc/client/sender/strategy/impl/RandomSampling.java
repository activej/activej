/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.rpc.client.sender.strategy.impl;

import io.activej.async.callback.Callback;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.client.sender.RpcSender;
import io.activej.rpc.client.sender.strategy.RpcStrategy;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.Checks.checkArgument;

@ExposedInternals
public final class RandomSampling implements RpcStrategy {
	public final Map<RpcStrategy, Double> strategyToWeight;

	public Random random;

	public RandomSampling(Random random, Map<RpcStrategy, Double> strategyToWeight) {
		this.random = random;
		this.strategyToWeight = strategyToWeight;
	}

	public static Builder builder() {
		return new RandomSampling(new Random(), new HashMap<>()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RandomSampling> {
		private Builder() {}

		public Builder with(double weight, RpcStrategy strategy) {
			checkNotBuilt(this);
			checkArgument(weight >= 0, "weight cannot be negative");
			checkArgument(!strategyToWeight.containsKey(strategy), "withStrategy is already added");
			strategyToWeight.put(strategy, weight);
			return this;
		}

		public Builder withRandom(Random random) {
			checkNotBuilt(this);
			RandomSampling.this.random = random;
			return this;
		}

		@Override
		protected RandomSampling doBuild() {
			return RandomSampling.this;
		}
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy strategy : strategyToWeight.keySet()) {
			result.addAll(strategy.getAddresses());
		}
		return result;
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		Map<RpcSender, Double> senderToWeight = new HashMap<>();
		double totalWeight = 0;
		for (Map.Entry<RpcStrategy, Double> entry : strategyToWeight.entrySet()) {
			RpcSender sender = entry.getKey().createSender(pool);
			if (sender != null) {
				double weight = entry.getValue();
				senderToWeight.put(sender, weight);
				totalWeight += weight;
			}
		}

		if (totalWeight == 0) {
			return null;
		}

		long randomLong = random.nextLong();
		long seed = randomLong != 0L ? randomLong : 2347230858016798896L;

		return new RandomSamplingSender(senderToWeight, seed, totalWeight);
	}

	public static final class RandomSamplingSender implements RpcSender {
		private final RpcSender[] senders;
		private final int[] cumulativeWeights;

		private long lastRandomLong;

		RandomSamplingSender(Map<RpcSender, Double> senderToWeight, long seed, double totalWeight) {
			assert !senderToWeight.containsKey(null);

			senders = new RpcSender[senderToWeight.size()];
			cumulativeWeights = new int[senderToWeight.size()];
			double currentCumulativeWeight = 0;
			int i = 0;
			for (Map.Entry<RpcSender, Double> entry : senderToWeight.entrySet()) {
				currentCumulativeWeight += entry.getValue();
				senders[i] = entry.getKey();
				cumulativeWeights[i] = (int) (currentCumulativeWeight / totalWeight * Integer.MAX_VALUE);
				i++;
			}

			lastRandomLong = seed;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			lastRandomLong ^= (lastRandomLong << 21);
			lastRandomLong ^= (lastRandomLong >>> 35);
			lastRandomLong ^= (lastRandomLong << 4);
			int currentRandomValue = (int) lastRandomLong & Integer.MAX_VALUE;
			int lowerIndex = 0;
			int upperIndex = cumulativeWeights.length;
			while (lowerIndex != upperIndex) {
				int middle = (lowerIndex + upperIndex) / 2;
				if (currentRandomValue >= cumulativeWeights[middle]) {
					lowerIndex = middle + 1;
				} else {
					upperIndex = middle;
				}
			}
			senders[lowerIndex].sendRequest(request, timeout, cb);
		}
	}

}
