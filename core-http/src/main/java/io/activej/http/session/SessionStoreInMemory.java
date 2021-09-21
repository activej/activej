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

import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple reference implementation of the session storage over a hash map.
 */
public final class SessionStoreInMemory<T> implements SessionStore<T>, WithInitializer<SessionStoreInMemory<T>> {
	private final Map<String, TWithTimestamp> store = new HashMap<>();

	private @Nullable Duration sessionLifetime;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private SessionStoreInMemory() {
	}

	public static <T> SessionStoreInMemory<T> create() {
		return new SessionStoreInMemory<>();
	}

	public SessionStoreInMemory<T> withLifetime(Duration sessionLifetime) {
		this.sessionLifetime = sessionLifetime;
		return this;
	}

	@Override
	public Promise<Void> save(String sessionId, T sessionObject) {
		store.put(sessionId, new TWithTimestamp(sessionObject, now.currentTimeMillis()));
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable T> get(String sessionId) {
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
