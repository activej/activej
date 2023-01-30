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
import io.activej.common.builder.AbstractBuilder;
import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.protocol.RpcException;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

import static io.activej.rpc.client.sender.Utils.listOfNullableSenders;

public final class RpcStrategy_Sharding implements RpcStrategy {
	private final List<? extends RpcStrategy> list;
	private final ToIntFunction<?> shardingFunction;
	private int minActiveSubStrategies;

	private RpcStrategy_Sharding(ToIntFunction<?> shardingFunction, List<? extends RpcStrategy> list, int minActiveSubStrategies) {
		this.shardingFunction = shardingFunction;
		this.list = list;
		this.minActiveSubStrategies = minActiveSubStrategies;
	}

	public static <T> RpcStrategy_Sharding of(ToIntFunction<T> shardingFunction, List<RpcStrategy> strategies, int minActiveSubStrategies) {
		return new RpcStrategy_Sharding(shardingFunction, strategies, minActiveSubStrategies);
	}

	public static <T> RpcStrategy_Sharding create(ToIntFunction<T> shardingFunction, RpcStrategy... strategies) {
		return builder(shardingFunction, strategies).build();
	}

	public static <T> RpcStrategy_Sharding create(ToIntFunction<T> shardingFunction, List<? extends RpcStrategy> strategies) {
		return builder(shardingFunction, strategies).build();
	}

	public static <T> Builder builder(ToIntFunction<T> shardingFunction, RpcStrategy... strategies) {
		return builder(shardingFunction, List.of(strategies));
	}

	public static <T> Builder builder(ToIntFunction<T> shardingFunction, List<? extends RpcStrategy> strategies) {
		return new RpcStrategy_Sharding(shardingFunction, strategies, 0).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RpcStrategy_Sharding> {
		private Builder() {}

		public Builder withMinActiveSubStrategies(int minActiveSubStrategies) {
			checkNotBuilt(this);
			RpcStrategy_Sharding.this.minActiveSubStrategies = minActiveSubStrategies;
			return this;
		}

		@Override
		protected RpcStrategy_Sharding doBuild() {
			return RpcStrategy_Sharding.this;
		}
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return Utils.getAddresses(list);
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		List<RpcSender> subSenders = listOfNullableSenders(list, pool);
		int activeSenders = 0;
		for (RpcSender subSender : subSenders) {
			if (subSender != null) {
				activeSenders++;
			}
		}
		if (activeSenders < minActiveSubStrategies)
			return null;
		if (subSenders.isEmpty())
			return null;
		if (subSenders.size() == 1)
			return subSenders.get(0);
		return new Sender(shardingFunction, subSenders);
	}

	public static final class Sender implements RpcSender {
		static final RpcException NO_SENDER_AVAILABLE_EXCEPTION = new RpcException("No senders available");

		private final ToIntFunction<Object> shardingFunction;
		private final RpcSender[] subSenders;

		Sender(ToIntFunction<?> shardingFunction, List<RpcSender> senders) {
			assert !senders.isEmpty();
			//noinspection unchecked
			this.shardingFunction = (ToIntFunction<Object>) shardingFunction;
			this.subSenders = senders.toArray(new RpcSender[0]);
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			int shardIndex = shardingFunction.applyAsInt(request);
			RpcSender sender = subSenders[shardIndex];
			if (sender != null) {
				sender.sendRequest(request, timeout, cb);
			} else {
				cb.accept(null, NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}

	}
}
