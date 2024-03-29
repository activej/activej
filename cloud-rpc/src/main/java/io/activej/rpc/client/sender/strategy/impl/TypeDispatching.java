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
import io.activej.rpc.protocol.RpcException;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.activej.common.Checks.checkState;

@ExposedInternals
public final class TypeDispatching implements RpcStrategy {
	public final Map<Class<?>, RpcStrategy> dataTypeToStrategy;
	public @Nullable RpcStrategy defaultStrategy;

	public TypeDispatching(Map<Class<?>, RpcStrategy> dataTypeToStrategy, @Nullable RpcStrategy defaultStrategy) {
		this.dataTypeToStrategy = dataTypeToStrategy;
		this.defaultStrategy = defaultStrategy;
	}

	public static Builder builder() {
		return new TypeDispatching(new HashMap<>(), null).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, TypeDispatching> {
		private Builder() {}

		public Builder with(Class<?> dataType, RpcStrategy strategy) {
			checkNotBuilt(this);
			checkState(!dataTypeToStrategy.containsKey(dataType),
				() -> "Strategy for type " + dataType.toString() + " is already set");
			dataTypeToStrategy.put(dataType, strategy);
			return this;
		}

		public Builder withDefault(RpcStrategy strategy) {
			checkNotBuilt(this);
			checkState(defaultStrategy == null, "Default Strategy is already set");
			defaultStrategy = strategy;
			return this;
		}

		@Override
		protected TypeDispatching doBuild() {
			return TypeDispatching.this;
		}
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy strategy : dataTypeToStrategy.values()) {
			result.addAll(strategy.getAddresses());
		}
		if (defaultStrategy != null) {
			result.addAll(defaultStrategy.getAddresses());
		}
		return result;
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		HashMap<Class<?>, RpcSender> typeToSender = new HashMap<>();
		for (Map.Entry<Class<?>, RpcStrategy> entry : dataTypeToStrategy.entrySet()) {
			RpcSender sender = entry.getValue().createSender(pool);
			if (sender == null)
				return null;
			typeToSender.put(entry.getKey(), sender);
		}
		RpcSender defaultSender = null;
		if (defaultStrategy != null) {
			defaultSender = defaultStrategy.createSender(pool);
			if (typeToSender.isEmpty())
				return defaultSender;
			if (defaultSender == null)
				return null;
		}
		return new Sender(typeToSender, defaultSender);
	}

	public static final class Sender implements RpcSender {
		static final RpcException NO_SENDER_AVAILABLE_EXCEPTION = new RpcException("No senders available");

		private final HashMap<Class<?>, RpcSender> typeToSender;
		private final @Nullable RpcSender defaultSender;

		Sender(HashMap<Class<?>, RpcSender> typeToSender, @Nullable RpcSender defaultSender) {
			this.typeToSender = typeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			RpcSender sender = typeToSender.get(request.getClass());
			if (sender == null) {
				sender = defaultSender;
			}
			if (sender != null) {
				sender.sendRequest(request, timeout, cb);
			} else {
				cb.accept(null, NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}
	}
}
