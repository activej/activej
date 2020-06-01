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

package io.activej.common.time;

import java.time.Instant;

import static io.activej.common.Preconditions.checkNotNull;

/**
 * Gives access to current time in milliseconds
 */
public interface CurrentTimeProvider {
	/**
	 * @return current time in milliseconds
	 */
	long currentTimeMillis();

	default Instant currentInstant() {
		return Instant.ofEpochMilli(currentTimeMillis());
	}

	static CurrentTimeProvider ofSystem() {
		return System::currentTimeMillis;
	}

	ThreadLocal<CurrentTimeProvider> THREAD_LOCAL_TIME_PROVIDER = new ThreadLocal<>();

	static CurrentTimeProvider ofThreadLocal() {
		return checkNotNull(THREAD_LOCAL_TIME_PROVIDER.get(), "ThreadLocal has no instance of CurrentTimeProvider associated with current thread");
	}

	static CurrentTimeProvider ofTimeSequence(long start, long increment) {
		return new CurrentTimeProvider() {
			long time = start;

			@Override
			public long currentTimeMillis() {
				long time = this.time;
				this.time += increment;
				return time;
			}
		};
	}

}
