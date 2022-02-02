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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

import static io.activej.rpc.client.sender.RpcStrategies.NO_SENDER_AVAILABLE_EXCEPTION;

public final class RpcStrategySharding implements RpcStrategy {
	private final List<RpcStrategy> list;
	private final ToIntFunction<?> shardingFunction;
	private final int minActiveSubStrategies;

	private RpcStrategySharding(@NotNull ToIntFunction<?> shardingFunction, @NotNull List<RpcStrategy> list, int minActiveSubStrategies) {
		this.shardingFunction = shardingFunction;
		this.list = list;
		this.minActiveSubStrategies = minActiveSubStrategies;
	}

	public static <T> RpcStrategySharding create(ToIntFunction<T> shardingFunction, @NotNull List<RpcStrategy> list) {
		return new RpcStrategySharding(shardingFunction, list, 0);
	}

	public static <T> RpcStrategySharding create(ToIntFunction<T> shardingFunction, RpcStrategy... list) {
		return new RpcStrategySharding(shardingFunction, List.of(list), 0);
	}

	public RpcStrategySharding withMinActiveSubStrategies(int minActiveSubStrategies) {
		return new RpcStrategySharding(shardingFunction, list, minActiveSubStrategies);
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return Utils.getAddresses(list);
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		List<RpcSender> subSenders = Utils.listOfNullableSenders(list, pool);
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

	private static final class Sender implements RpcSender {
		private final ToIntFunction<Object> shardingFunction;
		private final RpcSender[] subSenders;

		Sender(@NotNull ToIntFunction<?> shardingFunction, @NotNull List<RpcSender> senders) {
			assert !senders.isEmpty();
			//noinspection unchecked
			this.shardingFunction = (ToIntFunction<Object>) shardingFunction;
			this.subSenders = senders.toArray(new RpcSender[0]);
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
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
