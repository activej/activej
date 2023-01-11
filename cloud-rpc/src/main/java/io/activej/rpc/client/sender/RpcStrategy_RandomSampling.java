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

package io.activej.rpc.client.sender;

import io.activej.async.callback.Callback;
import io.activej.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.Checks.checkArgument;

public class RpcStrategy_RandomSampling implements RpcStrategy {
	private final Random random = new Random();
	private final Map<RpcStrategy, Integer> strategyToWeight = new HashMap<>();

	private RpcStrategy_RandomSampling() {}

	public static RpcStrategy_RandomSampling create() {return new RpcStrategy_RandomSampling();}

	public RpcStrategy_RandomSampling add(int weight, RpcStrategy strategy) {
		checkArgument(weight >= 0, "weight cannot be negative");
		checkArgument(!strategyToWeight.containsKey(strategy), "withStrategy is already added");

		strategyToWeight.put(strategy, weight);

		return this;
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
		Map<RpcSender, Integer> senderToWeight = new HashMap<>();
		int totalWeight = 0;
		for (Map.Entry<RpcStrategy, Integer> entry : strategyToWeight.entrySet()) {
			RpcSender sender = entry.getKey().createSender(pool);
			if (sender != null) {
				int weight = entry.getValue();
				senderToWeight.put(sender, weight);
				totalWeight += weight;
			}
		}

		if (totalWeight == 0) {
			return null;
		}

		long randomLong = random.nextLong();
		long seed = randomLong != 0L ? randomLong : 2347230858016798896L;

		return new RandomSamplingSender(senderToWeight, seed);
	}

	private static final class RandomSamplingSender implements RpcSender {
		private final List<RpcSender> senders;
		private final int[] cumulativeWeights;
		private final int totalWeight;

		private long lastRandomLong;

		RandomSamplingSender(Map<RpcSender, Integer> senderToWeight, long seed) {
			assert !senderToWeight.containsKey(null);

			senders = new ArrayList<>(senderToWeight.size());
			cumulativeWeights = new int[senderToWeight.size()];
			int currentCumulativeWeight = 0;
			int currentSender = 0;
			for (Map.Entry<RpcSender, Integer> entry : senderToWeight.entrySet()) {
				currentCumulativeWeight += entry.getValue();
				senders.add(entry.getKey());
				cumulativeWeights[currentSender++] = currentCumulativeWeight;
			}
			totalWeight = currentCumulativeWeight;

			lastRandomLong = seed;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			lastRandomLong ^= (lastRandomLong << 21);
			lastRandomLong ^= (lastRandomLong >>> 35);
			lastRandomLong ^= (lastRandomLong << 4);
			int currentRandomValue = (int) ((lastRandomLong & Long.MAX_VALUE) % totalWeight);
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
			senders.get(lowerIndex).sendRequest(request, timeout, cb);
		}
	}

}
