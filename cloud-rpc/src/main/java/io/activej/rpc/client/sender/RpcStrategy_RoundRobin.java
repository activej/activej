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
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

public final class RpcStrategy_RoundRobin implements RpcStrategy {
	private final List<RpcStrategy> list;
	private final int minActiveSubStrategies;

	private RpcStrategy_RoundRobin(List<RpcStrategy> list, int minActiveSubStrategies) {
		this.list = list;
		this.minActiveSubStrategies = minActiveSubStrategies;
	}

	public static RpcStrategy_RoundRobin create(RpcStrategy... list) {
		return create(List.of(list));
	}

	public static RpcStrategy_RoundRobin create(List<RpcStrategy> list) {
		return new RpcStrategy_RoundRobin(list, 0);
	}

	public RpcStrategy_RoundRobin withMinActiveSubStrategies(int minActiveSubStrategies) {
		return new RpcStrategy_RoundRobin(list, minActiveSubStrategies);
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

	private static final class Sender implements RpcSender {
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
