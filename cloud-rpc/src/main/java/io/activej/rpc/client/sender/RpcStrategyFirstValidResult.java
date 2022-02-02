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

public final class RpcStrategyFirstValidResult implements RpcStrategy {
	@FunctionalInterface
	public interface ResultValidator<T> {
		boolean isValidResult(T value);
	}

	private static final ResultValidator<?> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

	private final List<RpcStrategy> list;

	private final ResultValidator<?> resultValidator;
	private final @Nullable Exception noValidResultException;

	private RpcStrategyFirstValidResult(List<RpcStrategy> list, ResultValidator<?> resultValidator,
			@Nullable Exception noValidResultException) {
		this.list = list;
		this.resultValidator = resultValidator;
		this.noValidResultException = noValidResultException;
	}

	public static RpcStrategyFirstValidResult create(RpcStrategy... list) {
		return create(List.of(list));
	}

	public static RpcStrategyFirstValidResult create(List<RpcStrategy> list) {
		return new RpcStrategyFirstValidResult(list, DEFAULT_RESULT_VALIDATOR, null);
	}

	public RpcStrategyFirstValidResult withResultValidator(@NotNull ResultValidator<?> resultValidator) {
		return new RpcStrategyFirstValidResult(list, resultValidator, noValidResultException);
	}

	public RpcStrategyFirstValidResult withNoValidResultException(@NotNull Exception e) {
		return new RpcStrategyFirstValidResult(list, resultValidator, e);
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return Utils.getAddresses(list);
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		List<RpcSender> senders = Utils.listOfSenders(list, pool);
		if (senders.isEmpty())
			return null;
		return new Sender(senders, resultValidator, noValidResultException);
	}

	static final class Sender implements RpcSender {
		private final RpcSender[] subSenders;
		private final ResultValidator<?> resultValidator;
		private final @Nullable Exception noValidResultException;

		Sender(@NotNull List<RpcSender> senders, @NotNull ResultValidator<?> resultValidator,
				@Nullable Exception noValidResultException) {
			assert !senders.isEmpty();
			this.subSenders = senders.toArray(new RpcSender[0]);
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
			FirstResultCallback<O> firstResultCallback = new FirstResultCallback<>(subSenders.length, (ResultValidator<O>) resultValidator, cb, noValidResultException);
			for (RpcSender sender : subSenders) {
				sender.sendRequest(request, timeout, firstResultCallback);
			}
		}
	}

	static final class FirstResultCallback<T> implements Callback<T> {
		private int expectedCalls;
		private final ResultValidator<T> resultValidator;
		private final Callback<T> resultCallback;
		private Exception lastException;
		private final @Nullable Exception noValidResultException;

		FirstResultCallback(int expectedCalls, @NotNull ResultValidator<T> resultValidator, @NotNull Callback<T> resultCallback,
				@Nullable Exception noValidResultException) {
			assert expectedCalls > 0;
			this.expectedCalls = expectedCalls;
			this.resultCallback = resultCallback;
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		@Override
		public void accept(T result, @Nullable Exception e) {
			if (e == null) {
				if (--expectedCalls >= 0) {
					if (resultValidator.isValidResult(result)) {
						expectedCalls = 0;
						resultCallback.accept(result, null);
					} else {
						if (expectedCalls == 0) {
							resultCallback.accept(null, lastException != null ? lastException : noValidResultException);
						}
					}
				}
			} else {
				lastException = e; // last Exception
				if (--expectedCalls == 0) {
					resultCallback.accept(null, lastException);
				}
			}
		}
	}

	private static final class DefaultResultValidator<T> implements ResultValidator<T> {
		@Override
		public boolean isValidResult(T input) {
			return input != null;
		}
	}

}
