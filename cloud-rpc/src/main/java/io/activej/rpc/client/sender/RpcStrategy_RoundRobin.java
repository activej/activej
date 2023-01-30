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
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

public final class RpcStrategy_RoundRobin implements RpcStrategy {
	private final List<? extends RpcStrategy> list;
	private int minActiveSubStrategies;

	private RpcStrategy_RoundRobin(List<? extends RpcStrategy> list, int minActiveSubStrategies) {
		this.list = list;
		this.minActiveSubStrategies = minActiveSubStrategies;
	}

	public static RpcStrategy_RoundRobin create(RpcStrategy... strategies) {
		return builder(strategies).build();
	}

	public static RpcStrategy_RoundRobin create(List<? extends RpcStrategy> strategies) {
		return builder(strategies).build();
	}

	public static Builder builder(RpcStrategy... strategies) {
		return builder(List.of(strategies));
	}

	public static Builder builder(List<? extends RpcStrategy> strategies) {
		return new RpcStrategy_RoundRobin(strategies, 0).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RpcStrategy_RoundRobin> {
		private Builder() {}

		public Builder withMinActiveSubStrategies(int minActiveSubStrategies) {
			checkNotBuilt(this);
			RpcStrategy_RoundRobin.this.minActiveSubStrategies = minActiveSubStrategies;
			return this;
		}

		@Override
		protected RpcStrategy_RoundRobin doBuild() {
			return RpcStrategy_RoundRobin.this;
		}
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return Utils.getAddresses(list);
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		List<RpcSender> subSenders = Utils.listOfSenders(list, pool);
		if (subSenders.size() < minActiveSubStrategies)
			return null;
		if (subSenders.isEmpty())
			return null;
		if (subSenders.size() == 1)
			return subSenders.get(0);
		return new Sender(subSenders);
	}

	public static final class Sender implements RpcSender {
		private int nextSender;
		private final RpcSender[] subSenders;

		Sender(List<RpcSender> senders) {
			assert !senders.isEmpty();
			this.subSenders = senders.toArray(new RpcSender[0]);
			this.nextSender = 0;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			RpcSender sender = subSenders[nextSender];
			nextSender = (nextSender + 1) % subSenders.length;
			sender.sendRequest(request, timeout, cb);
		}

	}
}
