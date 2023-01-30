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

package io.activej.http.session;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * A simple reference implementation of the session storage over a hash map.
 */
public final class InMemorySessionStore<T> extends AbstractReactive
		implements ISessionStore<T> {
	private final Map<String, TWithTimestamp> store = new HashMap<>();

	private @Nullable Duration sessionLifetime;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private InMemorySessionStore(Reactor reactor) {
		super(reactor);
	}

	public static <T> InMemorySessionStore<T> create(Reactor reactor) {
		return InMemorySessionStore.<T>builder(reactor).build();
	}

	public static <T> InMemorySessionStore<T>.Builder builder(Reactor reactor) {
		return new InMemorySessionStore<T>(reactor).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, InMemorySessionStore<T>> {
		private Builder() {}

		public Builder withLifetime(Duration sessionLifetime) {
			checkNotBuilt(this);
			InMemorySessionStore.this.sessionLifetime = sessionLifetime;
			return this;
		}

		@Override
		protected InMemorySessionStore<T> doBuild() {
			return InMemorySessionStore.this;
		}
	}

	@Override
	public Promise<Void> save(String sessionId, T sessionObject) {
		checkInReactorThread(this);
		store.put(sessionId, new TWithTimestamp(sessionObject, now.currentTimeMillis()));
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable T> get(String sessionId) {
		checkInReactorThread(this);
		long timestamp = now.currentTimeMillis();
		TWithTimestamp tWithTimestamp = store.get(sessionId);
		if (tWithTimestamp == null) {
			return Promise.of(null);
		}
		if (sessionLifetime != null && tWithTimestamp.timestamp + sessionLifetime.toMillis() < timestamp) {
			store.remove(sessionId);
			return Promise.of(null);
		}
		tWithTimestamp.timestamp = timestamp;
		return Promise.of(tWithTimestamp.value);
	}

	@Override
	public Promise<Void> remove(String sessionId) {
		checkInReactorThread(this);
		store.remove(sessionId);
		return Promise.complete();
	}

	@Override
	public @Nullable Duration getSessionLifetimeHint() {
		return sessionLifetime;
	}

	private class TWithTimestamp {
		final T value;
		long timestamp;

		public TWithTimestamp(T value, long timestamp) {
			this.value = value;
			this.timestamp = timestamp;
		}
	}
}
