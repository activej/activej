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

package io.activej.ot;

import io.activej.async.function.AsyncSupplier;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;

import static io.activej.promise.RetryPolicy.exponentialBackoff;

public final class PollSanitizer<T> implements AsyncSupplier<T> {
	public static final Duration DEFAULT_YIELD_INTERVAL = Duration.ofMillis(1000L);

	private Duration yieldInterval = DEFAULT_YIELD_INTERVAL;

	private final AsyncSupplier<T> poll;

	@Nullable
	private T lastValue;

	private PollSanitizer(AsyncSupplier<T> poll) {
		this.poll = poll;
	}

	public static <T> PollSanitizer<T> create(AsyncSupplier<T> poll) {
		return new PollSanitizer<>(poll);
	}

	public PollSanitizer<T> withYieldInterval(Duration yieldInterval) {
		this.yieldInterval = yieldInterval;
		return this;
	}

	@Override
	public Promise<T> get() {
		return Promises.retry(poll,
				(value, e) -> {
					if (e != null) return true;
					if (Objects.equals(value, lastValue)) {
						return false;
					} else {
						this.lastValue = value;
						return true;
					}
				},
				exponentialBackoff(Duration.ofMillis(1), yieldInterval));
	}
}
