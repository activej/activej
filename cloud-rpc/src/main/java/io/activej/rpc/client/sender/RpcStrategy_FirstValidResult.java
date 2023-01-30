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
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public final class RpcStrategy_FirstValidResult implements RpcStrategy {
	static final Predicate<?> DEFAULT_RESULT_VALIDATOR = Objects::nonNull;

	private final List<? extends RpcStrategy> list;

	private Predicate<?> resultValidator;
	private @Nullable Exception noValidResultException;

	private RpcStrategy_FirstValidResult(List<? extends RpcStrategy> list, Predicate<?> resultValidator,
			@Nullable Exception noValidResultException) {
		this.list = list;
		this.resultValidator = resultValidator;
		this.noValidResultException = noValidResultException;
	}

	public static RpcStrategy_FirstValidResult of(List<RpcStrategy> list, Predicate<?> resultValidator, @Nullable Exception noValidResultException) {
		return new RpcStrategy_FirstValidResult(list, resultValidator, noValidResultException);
	}

	public static RpcStrategy_FirstValidResult create(RpcStrategy... list) {
		return builder(list).build();
	}

	public static RpcStrategy_FirstValidResult create(List<? extends RpcStrategy> list) {
		return builder(list).build();
	}

	public static Builder builder(RpcStrategy... list) {
		return builder(List.of(list));
	}

	public static Builder builder(List<? extends RpcStrategy> list) {
		return new RpcStrategy_FirstValidResult(list, DEFAULT_RESULT_VALIDATOR, null).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RpcStrategy_FirstValidResult> {
		private Builder() {}

		public Builder withResultValidator(Predicate<?> resultValidator) {
			checkNotBuilt(this);
			RpcStrategy_FirstValidResult.this.resultValidator = resultValidator;
			return this;
		}

		public Builder withNoValidResultException(Exception e) {
			checkNotBuilt(this);
			RpcStrategy_FirstValidResult.this.noValidResultException = e;
			return this;
		}

		@Override
		protected RpcStrategy_FirstValidResult doBuild() {
			return RpcStrategy_FirstValidResult.this;
		}
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

	public static final class Sender implements RpcSender {
		private final RpcSender[] subSenders;
		private final Predicate<?> resultValidator;
		private final @Nullable Exception noValidResultException;

		Sender(List<RpcSender> senders, Predicate<?> resultValidator,
				@Nullable Exception noValidResultException) {
			assert !senders.isEmpty();
			this.subSenders = senders.toArray(new RpcSender[0]);
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			FirstResultCallback<O> firstResultCallback = new FirstResultCallback<>(subSenders.length, (Predicate<O>) resultValidator, cb, noValidResultException);
			for (RpcSender sender : subSenders) {
				sender.sendRequest(request, timeout, firstResultCallback);
			}
		}
	}

	public static final class FirstResultCallback<T> implements Callback<T> {
		private int expectedCalls;
		private final Predicate<T> resultValidator;
		private final Callback<T> cb;
		private Exception lastException;
		private final @Nullable Exception noValidResultException;

		FirstResultCallback(int expectedCalls, Predicate<T> resultValidator, Callback<T> cb,
				@Nullable Exception noValidResultException) {
			assert expectedCalls > 0;
			this.expectedCalls = expectedCalls;
			this.cb = cb;
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		@Override
		public void accept(T result, @Nullable Exception e) {
			if (e == null) {
				if (--expectedCalls >= 0) {
					if (resultValidator.test(result)) {
						expectedCalls = 0;
						cb.accept(result, null);
					} else {
						if (expectedCalls == 0) {
							cb.accept(null, lastException != null ? lastException : noValidResultException);
						}
					}
				}
			} else {
				lastException = e; // last Exception
				if (--expectedCalls == 0) {
					cb.accept(null, lastException);
				}
			}
		}
	}

}
